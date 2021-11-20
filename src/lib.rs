#![forbid(unsafe_code)]

use slotmap::{HopSlotMap, new_key_type};

use std::collections::{HashSet, VecDeque};

new_key_type!{struct ComputeGraphKey;}

// TODO: generalize Vec<&T> -> T to Vec<&I> -> O
pub(crate) struct Node<T> {
    name: String,
    func: Box<dyn Fn(&[T]) -> T>,
    input_nodes: Vec<ComputeGraphKey>,
    output_cache: Option<T>
    // TODO: add &'a GraphToken maybe: omitting as Nodes will be SlotMap entries
}
// TODO: Remove Clone bound and use Arc<T> for return value instead?
impl<T: Clone> Node<T> {
    fn new(name: String, func: Box<dyn Fn(&[T]) -> T>) -> Node<T> {
        Node {
            name,
            func,
            input_nodes: Vec::default(),
            output_cache: None
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn eval(&mut self, args: &[T]) -> T {
        if self.output_cache.is_none() {
            self.output_cache = Some((self.func)(args));
        }
        if let Some(ref val) = self.output_cache {
            return val.clone();
        } else {
            panic!("Node cache is none despite computation completion");
        }
    }
    pub fn computed_val(&self) -> T {
        if let Some(ref val) = self.output_cache {
            return val.clone();
        } else {
            panic!("Node cache is none for computed_val call");
        }
    }
}

#[derive(Debug)]
pub struct NodeHandle {
    node_key: ComputeGraphKey,
    graph_id: usize
}

pub struct ComputationGraph<T> {
    node_storage: HopSlotMap<ComputeGraphKey, Node<T>>,
    //node_refcount:
    output_node: Option<ComputeGraphKey>,
    graph_id: usize
}
impl<T> Default for ComputationGraph<T> {
    fn default() -> Self {
        let mut obj = ComputationGraph {
            node_storage: HopSlotMap::default(),
            output_node: None,
            graph_id: 0
        };
        obj.graph_id = (&obj.node_storage as *const HopSlotMap<_,_>) as usize;
        obj
    }
}
impl<T: Clone> ComputationGraph<T> {
    pub fn new() -> ComputationGraph<T>{
        ComputationGraph::default()
    }
    pub fn insert_node(&mut self, name: String, func: Box<dyn Fn(&[T]) -> T>) -> NodeHandle {
        let node = Node::new(name, func);
        let node_key = self.node_storage.insert(node);
        NodeHandle {
            node_key,
            graph_id: self.graph_id
        }
    }
    pub fn designate_output(&mut self, node: &NodeHandle) {
        self.output_node.ok_or(()).expect_err("Output was already designated");
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        let node_key = node.node_key;
        assert!(self.node_storage.contains_key(node_key));
        self.output_node = Some(node_key);
    }
    pub fn set_inputs(&mut self, node: &mut NodeHandle, inputs: &[&NodeHandle]) {
        assert_eq!(node.graph_id, self.graph_id,
            "Received NodeHandle for different graph");
        let input_keys: Vec<_> = inputs.iter().map(|handle| handle.node_key).collect();
        assert!(!input_keys.contains(&node.node_key), "Inputs would create self-loop");
        // Other cycles would be caught at computation time
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
        self.node_storage.retain(|k, _| {
            sort_list.contains(&k)
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
        //let node_storage_mut = ;
        for node_key in self.computation_order() {
            let node = self.node_storage.get(node_key).unwrap();
            println!("Evaluating {}", node.name);

            let node_input_keyvec = node.input_nodes.clone();
            let node_inputs: Vec<_> = match node_input_keyvec.len() {
                0 => Vec::new(),
                _ => node_input_keyvec.into_iter().map(|key| {
                        self.node_storage.get(key).unwrap().computed_val()
                    }).collect()
            };
            // Rebind node as &mut to perform calculation
            let node = self.node_storage.get_mut(node_key).unwrap();
            node.eval(node_inputs.as_slice());
        }
        self.node_storage.get(self.output_node.take().unwrap()).unwrap().computed_val()
    }
}