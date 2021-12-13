use dag_compute::ComputationGraph;

use std::io::Write;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
enum HistogramFlow {
    RawString(String),
    Histogram(BTreeMap<char, usize>)
}

fn main() {
    let mut graph = ComputationGraph::<HistogramFlow>::new();
    let handle_in = graph.insert_node(
        "input".to_owned(),
        Box::new(|_| {
            print!("Enter string: ");
            std::io::stdout().flush().unwrap();
            let mut string_in = String::new();
            std::io::stdin().read_line(&mut string_in).unwrap();
            let trimmed = string_in.trim_end_matches('\n').to_owned();
            HistogramFlow::RawString(trimmed)
        })
    );
    let mut compute_histogram = graph.insert_node(
        "histogram".to_owned(),
        Box::new(|x| {
            if let HistogramFlow::RawString(s) = x[0] {
                let mut histogram: BTreeMap<char, usize> = BTreeMap::new();
                for char_val in s.chars() {
                    let entry = histogram.entry(char_val);
                    *entry.or_insert(0) += 1;
                }
                HistogramFlow::Histogram(histogram)
            } else {
                panic!("Expected RawString variant, got {:?}", x);
            }
        })
    );
    graph.set_inputs(&mut compute_histogram, &[&handle_in]);
    graph.designate_output(&compute_histogram);
    let final_val = graph.compute();
    if let HistogramFlow::Histogram(map) = final_val {
        for key in map.keys() {
            let count = map.get(key).unwrap();
            println!("{}: {}", key, count);
        }
    }
}