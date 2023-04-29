use ssb_flume_follower_sql::SsbQuery;

fn main() {
    let mut view = SsbQuery::new(
        "/home/dinosaur/.ssb/log.offset".into(),
        "/home/dinosaur/repos/ahdinosaur/ssb-archive/output.sqlite3".into(),
        Vec::new(),
        &"6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519",
    )
    .unwrap();

    while view.get_log_latest() != Some(view.get_view_latest()) {
        println!("log latest: {:?}", view.get_log_latest());
        println!("view latest: {:?}", view.get_view_latest());
        view.process(1000);
    }

    println!("Done!")
}
