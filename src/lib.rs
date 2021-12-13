#![forbid(unsafe_code)]

use slotmap::{SlotMap, SecondaryMap, new_key_type};

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::ops::Deref;

new_key_type!{struct ComputeGraphKey;}

// TODO: generalize Vec<&T> -> T to Vec<&I> -> O
type BoxedEvalFn<T> = Box<dyn Fn(&[&T]) -> T + Send + Sync>;

pub(crate) struct Node<T> {
    name: String,
    func: BoxedEvalFn<T>,
    input_nodes: Vec<ComputeGraphKey>,
    output_cache: Option<Arc<T>>
}
// TODO: Remove Clone bound and use Arc<T> for return value instead?
impl<T: Clone> Node<T> {
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
    pub fn eval(&mut self, args: &[&T]){
        if self.output_cache.is_none() {
            self.output_cache = Some(Arc::new((self.func)(args)));
        }
    }
    pub fn computed_val(&self) -> Arc<T> {
        if let Some(ref val) = self.output_cache {
            val.clone()
        } else {
            panic!("Node cache is none for computed_val call");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NodeHandle {
    node_key: ComputeGraphKey,
    graph_id: usize
}

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
        obj.graph_id = (&obj.node_storage as *const SlotMap<_,_>) as usize;
        obj
    }
}
impl<T: Clone> ComputationGraph<T> {
    pub fn new() -> ComputationGraph<T>{
        ComputationGraph::default()
    }
    pub fn insert_node(&mut self, name: String, func: BoxedEvalFn<T>) -> NodeHandle {
        let node = Node::new(name, func);
        let node_key = self.node_storage.insert(node);
        self.node_refcount.insert(node_key, 0);
        NodeHandle {
            node_key,
            graph_id: self.graph_id
        }
    }
    pub fn node_name(&self, node: &NodeHandle) -> String {
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        self.node_storage.get(node.node_key).unwrap().name.clone()
    }
    pub fn designate_output(&mut self, node: &NodeHandle) {
        self.output_node.ok_or(()).expect_err("Output was already designated");
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        let node_key = node.node_key;
        assert!(self.node_storage.contains_key(node_key));
        self.output_node = Some(node_key);
        *self.node_refcount.get_mut(node_key).unwrap() += 1;
    }
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

    fn computation_order(&mut self) -> impl IntoIterator<Item = ComputeGraphKey> {
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
                for input_key in &del_node.input_nodes {
                    *self.node_refcount.get_mut(*input_key).unwrap() -= 1;
                }
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

    pub fn compute(mut self) -> T {
        self.output_node.expect("Output not yet designated");
        for node_key in self.computation_order() {
            let node = self.node_storage.get(node_key).unwrap();
            println!("Evaluating {}", node.name);
            println!("{:?}", self.node_refcount);

            let node_input_keyvec = node.input_nodes.clone();
            let mut nodes_cleanup = Vec::with_capacity(node_input_keyvec.len());
            let node_input_arcs: Vec<_> = node_input_keyvec.into_iter().map(|key| {
                let in_refcnt = self.node_refcount.get_mut(key).unwrap();
                assert!(*in_refcnt > 0);
                *in_refcnt -= 1;
                if *in_refcnt == 0 {
                    nodes_cleanup.push(key);
                }
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
            // Toposort guarantees that inputs will be ready when needed
            println!("{:?}", self.node_refcount);
            node.eval(node_inputs.as_slice());
        }
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