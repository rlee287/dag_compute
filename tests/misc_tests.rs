use dag_compute::ComputationGraph;

#[test]
fn test_add_basic() {
    let mut graph = ComputationGraph::<i32>::new();
    let mut add_handle = graph.insert_node(
        "add".to_owned(),
        Box::new(|x| {
            let mut sum = 0;
            for item in x.iter() {
                sum += *item;
            }
            sum
        })
    );
    let handle_a = graph.insert_node(
        "a".to_owned(),
        Box::new(|_| {
            2
        })
    );
    let handle_b = graph.insert_node(
        "b".to_owned(),
        Box::new(|_| {
            4
        })
    );
    graph.set_inputs(&mut add_handle, &[&handle_a, &handle_b]);
    graph.designate_output(&add_handle);
    assert_eq!(graph.compute(), 6);
}

#[test]
fn test_incl_sweep() {
    let mut graph = ComputationGraph::<String>::new();
    let src = graph.insert_node(
        "const".to_owned(),
        Box::new(|_| "a".to_owned())
    );
    let mut incr_keep = graph.insert_node(
        "+1_out".to_owned(),
        Box::new(|s| s[0].clone()+"b")
    );
    graph.set_inputs(&mut incr_keep, &[&src]);
    let mut incr_toss = graph.insert_node(
        "+1_toss".to_owned(),
        Box::new(|s| s[0].clone()+"c")
    );
    graph.set_inputs(&mut incr_toss, &[&incr_keep]);
    graph.designate_output(&incr_keep);
    assert_eq!(graph.compute(), "ab")
}

#[test]
#[should_panic]
fn cycle_loop() {
    let mut graph = ComputationGraph::new();
    let mut handle_1 = graph.insert_node(
        "loopy_1".to_owned(),
        Box::new(|_| 5)
    );
    let mut handle_2 = graph.insert_node(
        "loopy_2".to_owned(),
        Box::new(|_| 5)
    );
    graph.set_inputs(&mut handle_1, &[&handle_2]);
    graph.set_inputs(&mut handle_2, &[&handle_1]);
    graph.designate_output(&handle_1);
    graph.compute();
}