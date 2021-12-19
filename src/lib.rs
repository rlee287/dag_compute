#![forbid(unsafe_code)]
#![doc(html_root_url = "https://docs.rs/dag_compute/0.1.0")]

use slotmap::{SlotMap, SecondaryMap, new_key_type};
use slotmap::Key as KeyTrait;

use std::collections::{HashSet, HashMap, VecDeque};
use std::sync::Arc;
use std::ops::Deref;
use std::marker::PhantomData;
use std::fmt;

use log::{info, debug, trace};

new_key_type!{struct ComputeGraphKey;}

type BoxedEvalFn<T> = Box<dyn Fn(&[&T]) -> T + Send + Sync>;

pub(crate) struct Node<T> {
    name: String,
    func: BoxedEvalFn<T>,
    input_nodes: Vec<ComputeGraphKey>,
    output_cache: Option<Arc<T>>
}
impl<T> Node<T> {
    fn new(name: String, func: BoxedEvalFn<T>) -> Node<T> {
        Node {
            name,
            func,
            input_nodes: Vec::default(),
            output_cache: None
        }
    }
    // Passing arg slice instead of node handles is a leaky encapsulation
    // Doesn't seem to be possible to remove leakiness safely though?
    pub fn eval(&mut self, args: &[&T]) {
        if self.output_cache.is_none() {
            self.output_cache = Some(Arc::new((self.func)(args)));
        } else {
            panic!("Node is already evaluated");
        }
    }
    pub fn computed_val(&self) -> Arc<T> {
        if let Some(ref val) = self.output_cache {
            val.clone()
        } else {
            panic!("Node has not yet been evaluated");
        }
    }
}

// DO NOT DERIVE Copy OR Clone: HANDLE MUST BE NON-FUNGIBLE
#[derive(Debug, PartialEq, Eq, Hash)]
/// An opaque handle to a node in a [`ComputationGraph`].
pub struct NodeHandle {
    node_key: ComputeGraphKey,
    graph_id: usize
}

/// A DAG that expresses a computation flow between nodes.
pub struct ComputationGraph<T> {
    node_storage: SlotMap<ComputeGraphKey, Node<T>>,
    node_refcount: SecondaryMap<ComputeGraphKey, u32>,
    output_node: Option<ComputeGraphKey>,
    graph_id: usize
}
impl<T> Default for ComputationGraph<T> {
    fn default() -> Self {
        let mut obj = ComputationGraph {
            node_storage: SlotMap::default(),
            node_refcount: SecondaryMap::default(),
            output_node: None,
            graph_id: 0
        };
        // Use pointer numerical value to tie NodeHandles to ComputationGraphs
        // No potential risks here as we only need this as an opaque token
        obj.graph_id = (&obj.node_storage as *const SlotMap<_,_>) as usize;
        obj
    }
}
impl<T> ComputationGraph<T> {
    pub fn new() -> ComputationGraph<T>{
        ComputationGraph::default()
    }
    /// Inserts a new node, returning an opaque node handle.
    /// 
    /// While the library does not enforce name uniqueness, this is
    /// highly recommended to make debugging easier.
    pub fn insert_node(&mut self, name: String, func: BoxedEvalFn<T>) -> NodeHandle {
        let node = Node::new(name, func);
        let node_key = self.node_storage.insert(node);
        self.node_refcount.insert(node_key, 0);
        NodeHandle {
            node_key,
            graph_id: self.graph_id
        }
    }
    /// Returns a reference to a node's name.
    pub fn node_name(&self, node: &NodeHandle) -> &str {
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        &self.node_storage.get(node.node_key).unwrap().name
    }
    /// Designates the given node as the output node.
    pub fn designate_output(&mut self, node: &NodeHandle) {
        self.output_node.ok_or(()).expect_err("Output was already designated");
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        let node_key = node.node_key;
        assert!(self.node_storage.contains_key(node_key));
        self.output_node = Some(node_key);
        *self.node_refcount.get_mut(node_key).unwrap() += 1;
    }
    /// Sets the given node's inputs.
    /// 
    /// It is the caller's responsibility to avoid creating loops,
    /// which are only detected at computation time.
    pub fn set_inputs(&mut self, node: &mut NodeHandle, inputs: &[&NodeHandle]) {
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        let input_keys: Vec<_> = inputs.iter().map(|handle| handle.node_key).collect();
        // Mutability rules actually enforce the non-circular-loop case
        // Keep assert in case duplication happens elsewhere
        assert!(!input_keys.contains(&node.node_key), "Inputs would create self-loop");
        // Other cycles would be caught at computation time

        for key in input_keys.iter() {
            *self.node_refcount.get_mut(*key).unwrap() += 1;
        }
        self.node_storage.get_mut(node.node_key).unwrap().input_nodes = input_keys;
    }
    /// Emits a DOT graph of the computation graph.
    /// 
    /// Nodes are labeled with names, and the output node is rectangular.
    pub fn dot_graph(&self) -> impl fmt::Display + '_ {
        DAGComputeDisplay::new(self)
    }

    /// Determines a valid order for node evaluation.
    fn computation_order(&mut self) -> impl IntoIterator<Item = ComputeGraphKey> {
        debug!("Computing node evaluation order");
        let out_node = self.output_node.expect("Output not yet designated");

        // Toposort the graph, marking used nodes
        let mut sort_list = VecDeque::new();
        let mut temporary_set = HashSet::new();
        self.toposort_helper(out_node, &mut sort_list, &mut temporary_set);
        debug_assert!(temporary_set.is_empty());

        // Sweep phase of mark-and-sweep GC
        self.node_storage.retain(|k, del_node| {
            let keep = sort_list.contains(&k);
            if !keep {
                trace!("Sweeping node {}", del_node.name);
                for input_key in &del_node.input_nodes {
                    *self.node_refcount.get_mut(*input_key).unwrap() -= 1;
                }
                self.node_refcount.remove(k);
            } else {
                trace!("Keeping node {}", del_node.name)
            }
            keep
        });
        /*
         * We traversed the edge in the opposite direction of the dataflow
         * Reverse now to get the correct directions
         * WARNING: this is valid for DFS-obtained toposort but not in general
         */
        sort_list.make_contiguous().reverse();
        sort_list
    }
    // Adapted from the DFS-based toposort of https://en.wikipedia.org/wiki/Topological_sorting
    fn toposort_helper(&self, node: ComputeGraphKey,
            final_list: &mut VecDeque<ComputeGraphKey>,
            temporary_set: &mut HashSet<ComputeGraphKey>) {
        if final_list.contains(&node) {
            return;
        }
        assert!(!temporary_set.contains(&node), "Computation graph contains cycle");
        temporary_set.insert(node);
        for input in self.node_storage.get(node).unwrap().input_nodes.iter() {
            self.toposort_helper(*input, final_list, temporary_set);
        }
        temporary_set.remove(&node);
        final_list.insert(0, node);
    }

    /// Computes and returns the value of the output node.
    pub fn compute(mut self) -> T {
        self.output_node.expect("Output not yet designated");
        info!("Evaluating DAG");
        let compute_order = self.computation_order();
        debug!("Computing node values");
        for node_key in compute_order {
            let node = self.node_storage.get(node_key).unwrap();
            trace!("Evaluating node {}", node.name);

            let node_input_keyvec = node.input_nodes.clone();
            let mut nodes_cleanup = Vec::with_capacity(node_input_keyvec.len());
            let node_input_arcs: Vec<_> = node_input_keyvec.into_iter().map(|key| {
                let in_refcnt = self.node_refcount.get_mut(key).unwrap();
                assert!(*in_refcnt > 0);
                *in_refcnt -= 1;
                if *in_refcnt == 0 {
                    nodes_cleanup.push(key);
                }
                // Toposort guarantees that inputs will be ready when needed
                self.node_storage.get(key).unwrap().computed_val()
            }).collect();
            // The refs in node_inputs are live as long as node_input_arcs is
            let mut node_inputs = Vec::with_capacity(node_input_arcs.len());
            for arc in node_input_arcs.iter() {
                node_inputs.push(arc.deref());
            }

            for old_key in nodes_cleanup {
                self.node_storage.remove(old_key);
                self.node_refcount.remove(old_key);
            }
            // Rebind node as &mut to perform calculation
            let node = self.node_storage.get_mut(node_key).unwrap();
            node.eval(node_inputs.as_slice());
        }
        // Assert checks that only the output node is left
        assert_eq!(self.node_storage.len(), 1);
        let output_key = self.output_node.take().unwrap();
        // Remove instead of get because we want an owned Node
        let output_node = self.node_storage.remove(output_key).unwrap();
        let output_val_arc = output_node.computed_val();
        drop(output_node);
        /*
         * We just computed the output value and didn't hand it to anyone else
         * We dropped the output node, which would have held the only other copy
         * There is exactly one copy of the Arc, so try_unwrap must succeed
         */
        Arc::try_unwrap(output_val_arc).ok().unwrap()
    }
}

struct DAGComputeDisplay<'a, T> {
    /*
     * We only really need edge_list, but hold a PhantomData to slotmap_ref
     * This prevents changes to the DAG so we only need to compute stuff once
     */
    // TODO: make this actual ref?
    slotmap_ref: PhantomData<&'a SlotMap<ComputeGraphKey, Node<T>>>,
    names: HashMap<ComputeGraphKey, &'a str>,
    output_node: Option<ComputeGraphKey>,
    edge_list: Vec<(ComputeGraphKey, ComputeGraphKey)>
}
impl<'a, T> DAGComputeDisplay<'a, T> {
    fn new(map: &'a ComputationGraph<T>) -> DAGComputeDisplay<'a, T> {
        let true_keyset: HashMap<ComputeGraphKey, &'a str> = map.node_storage
            .keys()
            .map(|key| (key, map.node_storage.get(key).unwrap().name.as_str()))
            .collect();
        let mut explored_keyset: HashSet<ComputeGraphKey> = HashSet::new();
        let mut edge_list = Vec::new();
        // len is more efficient than full equality
        // We need this to account for ill-formed graphs (don't reject here)
        while true_keyset.len() > explored_keyset.len() {
            debug_assert!(explored_keyset.is_subset(
                &true_keyset.keys().copied().collect()));
            // Do BFS to make the final dot file more human-readable
            let mut bfs_queue: VecDeque<ComputeGraphKey> = VecDeque::new();
            let mut bfs_root: Option<ComputeGraphKey> = None;
            for key in true_keyset.keys() {
                if !explored_keyset.contains(key) {
                    bfs_root = Some(*key);
                    break;
                }
            }
            let bfs_root = bfs_root.unwrap(); // Rebind and assert

            bfs_queue.push_back(bfs_root);
            explored_keyset.insert(bfs_root);
            while !bfs_queue.is_empty() {
                let current = bfs_queue.pop_front().unwrap();
                for input in map.node_storage.get(current).unwrap()
                        .input_nodes.iter() {
                    edge_list.push((*input, current));
                    // Insert returns true if new element was added
                    if explored_keyset.insert(*input) {
                        bfs_queue.push_back(*input);
                    }
                }
            }
        }
        debug_assert_eq!(true_keyset.keys().copied().collect::<HashSet<_>>(),
                explored_keyset);
        DAGComputeDisplay {
            slotmap_ref: PhantomData::default(),
            names: true_keyset,
            output_node: map.output_node,
            edge_list
        }
    }
}
impl<'a, T> fmt::Display for DAGComputeDisplay<'a, T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(fmt, "strict digraph {{")?;
        for (node, name) in self.names.iter() {
            let node_id = node.data().as_ffi();
            let escaped_name: String = name.chars().map(|c| {
                match c {
                    '"' => r#"\""#.to_owned(),
                    c => c.to_string()
                }
            }).collect();
            write!(fmt, "{} [label=\"{}\"", node_id, escaped_name)?;
            if let Some(out) = self.output_node {
                if out == *node {
                    write!(fmt, ", shape=box")?;
                }
            }
            writeln!(fmt, "];")?;
        }
        for edge in self.edge_list.iter() {
            // Use the u64 as_ffi to handle duplicate names
            let from_id = edge.0.data().as_ffi();
            let to_id = edge.1.data().as_ffi();
            writeln!(fmt, "{}->{};", from_id, to_id)?;
        }
        writeln!(fmt, "}}")
    }
}