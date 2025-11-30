use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use xml2arrow::PathTrieBuilder;
use xml2arrow::config::{Config, DType, FieldConfigBuilder, TableConfig};

/// Generate a configuration with varying complexity
fn generate_config(num_tables: usize, fields_per_table: usize, max_depth: usize) -> Config {
    let mut tables = Vec::with_capacity(num_tables);

    for table_idx in 0..num_tables {
        let table_path = format!(
            "/document/data/section{}/tables/table{}",
            table_idx % 10,
            table_idx
        );

        let mut fields = Vec::with_capacity(fields_per_table);

        // Add element fields
        for field_idx in 0..fields_per_table / 2 {
            let mut path_parts = table_path.clone();
            for depth_idx in 0..max_depth {
                path_parts.push_str(&format!("/level{}", depth_idx));
            }
            path_parts.push_str(&format!("/field{}", field_idx));

            fields.push(
                FieldConfigBuilder::new(
                    &format!("field_{}_{}", table_idx, field_idx),
                    &path_parts,
                    if field_idx % 3 == 0 {
                        DType::Float64
                    } else if field_idx % 3 == 1 {
                        DType::Int32
                    } else {
                        DType::Utf8
                    },
                )
                .nullable(field_idx % 2 == 0)
                .build(),
            );
        }

        // Add attribute fields
        for field_idx in fields_per_table / 2..fields_per_table {
            let attr_path = format!("{}/item/@attr{}", table_path, field_idx);

            fields.push(
                FieldConfigBuilder::new(
                    &format!("attr_{}_{}", table_idx, field_idx),
                    &attr_path,
                    DType::Utf8,
                )
                .build(),
            );
        }

        tables.push(TableConfig::new(
            &format!("table_{}", table_idx),
            &table_path,
            vec![],
            fields,
        ));
    }

    Config {
        parser_options: Default::default(),
        tables,
    }
}

fn bench_trie_construction_small(c: &mut Criterion) {
    let config = generate_config(5, 10, 3);

    c.bench_function("trie_construction_small_5t_10f_3d", |b| {
        b.iter(|| {
            let trie = PathTrieBuilder::from_config(black_box(&config)).unwrap();
            black_box(trie);
        });
    });
}

fn bench_trie_construction_medium(c: &mut Criterion) {
    let config = generate_config(20, 20, 5);

    c.bench_function("trie_construction_medium_20t_20f_5d", |b| {
        b.iter(|| {
            let trie = PathTrieBuilder::from_config(black_box(&config)).unwrap();
            black_box(trie);
        });
    });
}

fn bench_trie_construction_large(c: &mut Criterion) {
    let config = generate_config(50, 30, 7);

    c.bench_function("trie_construction_large_50t_30f_7d", |b| {
        b.iter(|| {
            let trie = PathTrieBuilder::from_config(black_box(&config)).unwrap();
            black_box(trie);
        });
    });
}

fn bench_trie_lookup_performance(c: &mut Criterion) {
    use string_cache::DefaultAtom as Atom;

    let config = generate_config(10, 20, 5);
    let trie = PathTrieBuilder::from_config(&config).unwrap();

    let mut group = c.benchmark_group("trie_lookup");

    // Benchmark element transitions
    group.bench_function("element_transition", |b| {
        let state = trie.root_id();
        let atom = Atom::from("document");
        b.iter(|| {
            let next = trie.transition_element(black_box(state), black_box(&atom));
            black_box(next);
        });
    });

    // Benchmark attribute transitions
    let mut state = trie.root_id();
    state = trie.transition_element(state, &Atom::from("document"));
    state = trie.transition_element(state, &Atom::from("data"));
    state = trie.transition_element(state, &Atom::from("section0"));
    state = trie.transition_element(state, &Atom::from("tables"));
    state = trie.transition_element(state, &Atom::from("table0"));
    state = trie.transition_element(state, &Atom::from("item"));

    if trie.has_attributes(state) {
        group.bench_function("attribute_transition", |b| {
            let attr = Atom::from("attr5");
            b.iter(|| {
                let next = trie.transition_attribute(black_box(state), black_box(&attr));
                black_box(next);
            });
        });
    }

    // Benchmark state property checks
    group.bench_function("is_table_root", |b| {
        b.iter(|| {
            let result = trie.is_table_root(black_box(state));
            black_box(result);
        });
    });

    group.bench_function("get_field_id", |b| {
        b.iter(|| {
            let result = trie.get_field_id(black_box(state));
            black_box(result);
        });
    });

    group.finish();
}

fn bench_trie_path_navigation(c: &mut Criterion) {
    use string_cache::DefaultAtom as Atom;

    let config = generate_config(10, 20, 5);
    let trie = PathTrieBuilder::from_config(&config).unwrap();

    let mut group = c.benchmark_group("path_navigation");

    // Benchmark shallow path (3 levels)
    group.bench_function("shallow_path_3_levels", |b| {
        b.iter(|| {
            let mut state = trie.root_id();
            state = trie.transition_element(state, &Atom::from("document"));
            state = trie.transition_element(state, &Atom::from("data"));
            state = trie.transition_element(state, &Atom::from("section0"));
            black_box(state);
        });
    });

    // Benchmark medium path (6 levels)
    group.bench_function("medium_path_6_levels", |b| {
        b.iter(|| {
            let mut state = trie.root_id();
            state = trie.transition_element(state, &Atom::from("document"));
            state = trie.transition_element(state, &Atom::from("data"));
            state = trie.transition_element(state, &Atom::from("section0"));
            state = trie.transition_element(state, &Atom::from("tables"));
            state = trie.transition_element(state, &Atom::from("table0"));
            state = trie.transition_element(state, &Atom::from("level0"));
            black_box(state);
        });
    });

    // Benchmark deep path (10 levels)
    group.bench_function("deep_path_10_levels", |b| {
        b.iter(|| {
            let mut state = trie.root_id();
            state = trie.transition_element(state, &Atom::from("document"));
            state = trie.transition_element(state, &Atom::from("data"));
            state = trie.transition_element(state, &Atom::from("section0"));
            state = trie.transition_element(state, &Atom::from("tables"));
            state = trie.transition_element(state, &Atom::from("table0"));
            state = trie.transition_element(state, &Atom::from("level0"));
            state = trie.transition_element(state, &Atom::from("level1"));
            state = trie.transition_element(state, &Atom::from("level2"));
            state = trie.transition_element(state, &Atom::from("level3"));
            state = trie.transition_element(state, &Atom::from("level4"));
            black_box(state);
        });
    });

    group.finish();
}

fn bench_trie_vs_string_comparison(c: &mut Criterion) {
    use string_cache::DefaultAtom as Atom;

    let config = generate_config(10, 20, 5);
    let trie = PathTrieBuilder::from_config(&config).unwrap();

    let mut group = c.benchmark_group("trie_vs_string");

    // Benchmark trie-based lookup
    group.bench_function("trie_lookup", |b| {
        b.iter(|| {
            let mut state = trie.root_id();
            state = trie.transition_element(state, &Atom::from("document"));
            state = trie.transition_element(state, &Atom::from("data"));
            state = trie.transition_element(state, &Atom::from("section0"));
            state = trie.transition_element(state, &Atom::from("tables"));
            state = trie.transition_element(state, &Atom::from("table0"));
            let field_id = trie.get_field_id(state);
            black_box(field_id);
        });
    });

    // Benchmark string comparison (simulating old approach)
    group.bench_function("string_comparison", |b| {
        let target = "/document/data/section0/tables/table0";
        b.iter(|| {
            let mut path = String::with_capacity(64);
            path.push_str("/document");
            path.push_str("/data");
            path.push_str("/section0");
            path.push_str("/tables");
            path.push_str("/table0");
            let matches = path == target;
            black_box(matches);
        });
    });

    group.finish();
}

fn bench_trie_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_scaling");

    for num_tables in [1, 5, 10, 25, 50, 100].iter() {
        let config = generate_config(*num_tables, 10, 3);

        group.bench_with_input(
            BenchmarkId::from_parameter(num_tables),
            num_tables,
            |b, _| {
                b.iter(|| {
                    let trie = PathTrieBuilder::from_config(black_box(&config)).unwrap();
                    black_box(trie);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_trie_construction_small,
    bench_trie_construction_medium,
    bench_trie_construction_large,
    bench_trie_lookup_performance,
    bench_trie_path_navigation,
    bench_trie_vs_string_comparison,
    bench_trie_scaling,
);

criterion_main!(benches);
