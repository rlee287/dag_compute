use dag_compute::ComputationGraph;

use std::io::Write;

fn read_in_i32() -> i32 {
    let stdin = std::io::stdin();
    let mut in_str = String::new();
    stdin.read_line(&mut in_str).unwrap();
    str::parse::<i32>(&in_str[..in_str.len()-1]).unwrap()
}

fn main() {
    // We comput a*b+c
    let mut graph = ComputationGraph::<i32>::new();
    let mut mult_handle = graph.insert_node("mult".to_owned(),
        Box::new(|x| {
            let mut prod = 1;
            for item in x.iter() {
                println!("prod *= {}", item);
                prod *= item;
            }
            println!("prod = {}", prod);
            prod
        })
    );
    let mut add_handle = graph.insert_node("add".to_owned(),
        Box::new(|x| {
            let mut sum = 0;
            for item in x.iter() {
                println!("sum += {}", item);
                sum += item;
            }
            println!("sum = {}", sum);
            sum
        })
    );
    println!("Evaluating a*b+c");
    let handle_a = graph.insert_node(
        "a".to_owned(),
        Box::new(|_| {
            print!("Enter value for a: ");
            std::io::stdout().flush().unwrap();
            read_in_i32()
        })
    );
    let handle_b = graph.insert_node(
        "b".to_owned(),
        Box::new(|_| {
            print!("Enter value for b: ");
            std::io::stdout().flush().unwrap();
            read_in_i32()
        })
    );
    let handle_c = graph.insert_node(
        "c".to_owned(),
        Box::new(|_| {
            print!("Enter value for c: ");
            std::io::stdout().flush().unwrap();
            read_in_i32()
        })
    );
    graph.set_inputs(&mut mult_handle, &[&handle_a, &handle_b]);
    graph.set_inputs(&mut add_handle, &[&mult_handle, &handle_c]);
    graph.designate_output(&add_handle);
    let final_val = graph.compute();
    println!("{}", final_val);
}