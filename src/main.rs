extern crate lz4;
use std::{thread,time};
use std::io::{stdout, stdin, Read, Write, Result, BufRead};
use std::fs::File;

// use zstd::stream::write::{Encoder, AutoFinishEncoder};
use lz4::EncoderBuilder;
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender};
use std::os::unix::io::{FromRawFd, AsRawFd};

fn create_encoder<T: Write>(level: usize, output: T) -> Result<lz4::Encoder<T>> {
   EncoderBuilder::new()
       .checksum(lz4::liblz4::ContentChecksum::NoChecksum)
       .block_mode(lz4::liblz4::BlockMode::Linked)
       .level(2)
       .build(output)
}

fn restart_encoder<T: Write>(level: usize, encoder: lz4::Encoder<T>) -> Result<lz4::Encoder<T>> {
  let (writer, _) = encoder.finish();
  create_encoder(level, writer)
}

// #[tokmain]
fn main() -> Result<()> {
    let my_stdin = stdin();
    let mut my_stdin = my_stdin.lock();

    let (return_sender, return_receiver): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(10);
    let (data_sender, data_receiver): (Sender<(Vec<u8>, usize)>, Receiver<(Vec<u8>, usize)>) = channel();
    for _i in 0..10 {
        return_sender.send(vec![0;16*1024*1024]).unwrap();
    }

    //// let mut encoder = Encoder::new(stdout(), 9)?.auto_finish();
    let child = thread::spawn(move|| {
        if let Ok(mut encoder) = create_encoder(2, stdout()) {
          let mut waited = 0;
          loop {
            waited += 1;
            if let Ok((recvd, len)) = data_receiver.recv_timeout(time::Duration::from_millis(300)) {
              waited = 0;
              if len == 0 { encoder.finish(); break }
              let mut written = 0;
              while written != len {
                if let Ok(last_write) = encoder.write(&recvd[written..len-written]) {
                  written += last_write;
                } else {
                }
              }
              if let Err(_) = return_sender.send(recvd) {
              }
            } else if waited == 1 {
              eprintln!("Flusing");
              if let Err(e) = encoder.flush() {
                eprintln!("Failed! {}", e);
              }
            } else if waited == 5 {
              eprintln!("Restarting encoder");
              let result = restart_encoder(2, encoder);
              if let Ok(newencoder) = result {
                encoder = newencoder;
                if let Err(e) = encoder.flush() {
                  eprintln!("Failed! {}", e);
                }
              } else {
                return;
              }
            }
          }
        } else {
          return;
        }
    });

    loop {
        match return_receiver.recv() {
            Ok(mut buf) => {
              let len = my_stdin.read(&mut buf[..])?;
              //let new_buf = my_stdin.fill_buf()?;
              //let msg = Vec::from(new_buf);
              if let Err(e) = data_sender.send((buf, len)) {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
              }
              if len == 0 { break }

            },
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        }
    }
    if let Err(e) = child.join() {
    }
    Ok(())
}
