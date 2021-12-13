use dag_compute::ComputationGraph;

use rand::prelude::*;
use rand::distributions::Uniform;

use std::fs::File;
use std::io::Write;

const SAMPLE_COUNT: usize = 96000;
const SAMPLE_RATE: u32 = 48000;

fn main() {
    let mut graph = ComputationGraph::<Option<[f32; SAMPLE_COUNT]>>::new();
    let noisegen_handle = graph.insert_node(
        "Noise generator".to_owned(),
        Box::new(|_| {
            let range = Uniform::new_inclusive(-0.25, 0.25);
            let mut rng = SmallRng::from_entropy();
            let mut noise_sample = [0.0; SAMPLE_COUNT];
            for arr_ptr in noise_sample.iter_mut() {
                *arr_ptr = range.sample(&mut rng);
            }
            Some(noise_sample)
        })
    );
    let mut filter_handle = graph.insert_node(
        "Boxcar filter".to_owned(),
        Box::new(|arr| {
            assert_eq!(arr.len(), 1);
            let window_length: usize = (SAMPLE_RATE/500) as usize;
            let mut data_tmp: Vec<f32> = vec![0.0; SAMPLE_COUNT+window_length-1];
            data_tmp[window_length-1..].copy_from_slice(&arr[0].unwrap());
            // Boxcar filter: inefficient but suffices to demonstrate
            let final_data_vec: Vec<_> = data_tmp.windows(window_length).map(
                    |window| {
                        let mut avg = 0.0;
                        for item in window {
                            avg += item;
                        }
                        avg /= window_length as f32;
                        avg
                    }
                )
                .map(|x| x*f32::sqrt(window_length as f32/2.0))
                .collect();
            assert_eq!(final_data_vec.len(), SAMPLE_COUNT);
            let mut final_data = [0.0; SAMPLE_COUNT];
            final_data.copy_from_slice(&final_data_vec);
            Some(final_data)
        })
    );
    graph.set_inputs(&mut filter_handle, &[&noisegen_handle]);
    let mut outputfile_handle = graph.insert_node(
        "Write output file".to_owned(),
        Box::new(|arrs| {
            assert_eq!(arrs.len(), 2);
            let wav_header = wav::Header::new(
                wav::WAV_FORMAT_IEEE_FLOAT,
                1,
                SAMPLE_RATE,
                32
            );

            let vec_raw_data: Vec<f32> = arrs[0].unwrap()
                .iter().copied().collect();
            let raw_data = wav::BitDepth::from(vec_raw_data);
            let mut raw_file = File::create("noise.wav").unwrap();
            wav::write(wav_header, &raw_data, &mut raw_file).unwrap();
            raw_file.flush().unwrap();
            drop(raw_file);

            let vec_filt_data: Vec<f32> = arrs[1].unwrap()
                .iter().copied().collect();
            let filt_data = wav::BitDepth::from(vec_filt_data);
            let mut filt_file = File::create("noise_filtered.wav").unwrap();
            wav::write(wav_header, &filt_data, &mut filt_file).unwrap();
            filt_file.flush().unwrap();
            drop(filt_file);
            None
        })
    );
    graph.set_inputs(&mut outputfile_handle,
        &[&noisegen_handle, &filter_handle]);
    graph.designate_output(&outputfile_handle);

    graph.compute();
}