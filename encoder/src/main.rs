extern crate lz4;
use std::{thread,time};
use std::io::{stdout, stdin, Read, Write, Result, BufReader, BufRead};
use std::env::args;
use std::fs::File;

// use zstd::stream::write::{Encoder, AutoFinishEncoder};
use lz4::EncoderBuilder;
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender};
use std::os::unix::io::{FromRawFd, AsRawFd};

fn create_encoder<T: Write>(level: usize, block_size: lz4::BlockSize, output: T) -> Result<lz4::Encoder<T>> {
   EncoderBuilder::new()
       .checksum(lz4::liblz4::ContentChecksum::NoChecksum)
       .block_mode(lz4::liblz4::BlockMode::Linked)
       .block_size(block_size)
       .level(2)
       .build(output)
}

fn restart_encoder<T: Write>(level: usize, block_size: lz4::BlockSize, encoder: lz4::Encoder<T>) -> Result<lz4::Encoder<T>> {
  let mut encoder = encoder;
  let (writer, _) = encoder.finish();
  create_encoder(level, block_size, writer)
}

enum WorkerPackage {
    Data(Vec<u8>, usize),
    SetLevel(usize),
    SetBlockSize(usize),
}

//fn main() -> Result<()> {
//
//}

fn main() -> Result<()> {
    let my_stdin = stdin();
    let mut my_stdin = my_stdin.lock();
    
    let mut args = args();
    let program_name = args.next();
    let pipe_file = args.next();
    if pipe_file.is_none() {
      println!("Usage: {} command_pipe_file", program_name.unwrap_or("lz4-streamer".to_string()));
      return Ok(());
    }
    let pipe_file = File::open(pipe_file.unwrap())?;

    let block_sizes = [lz4::BlockSize::Max4MB
                      ,lz4::BlockSize::Max1MB
                      ,lz4::BlockSize::Max256KB
                      ,lz4::BlockSize::Max64KB];
    let mut active_block_size = 3;
    let buffer_size = 64*1024;
    let num_buffers = 1024;

    //let (return_sender, return_receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();
    let (data_sender, data_receiver): (SyncSender<WorkerPackage>, Receiver<WorkerPackage>) = sync_channel(num_buffers);
    let level_sender = data_sender.clone();
    //for _i in 0..num_buffers {
    //    return_sender.send(vec![0;buffer_size]).unwrap();
    //}

    thread::spawn(move|| {
      let mut reader = BufReader::new(pipe_file);
      loop {
        let mut line = String::new();
        if let Ok(len) = reader.read_line(&mut line) {
          if len == 0 {
            break;
          }
          let mut line = line.trim_end();
          if let Ok(newlevel) = line.parse() {
            level_sender.send(WorkerPackage::SetLevel(newlevel)).unwrap();
          } else {
            match line {
              "b64K" => level_sender.send(WorkerPackage::SetBlockSize(3)).unwrap(),
              "b256K" => level_sender.send(WorkerPackage::SetBlockSize(2)).unwrap(),
              "b1M" => level_sender.send(WorkerPackage::SetBlockSize(1)).unwrap(),
              "b4M" => level_sender.send(WorkerPackage::SetBlockSize(0)).unwrap(),
              _ => (),
             }
          }

        }
      }
    });

    //// let mut encoder = Encoder::new(stdout(), 9)?.auto_finish();
    let child = thread::spawn(move|| {
        let mut level = 2;
        if let Ok(mut encoder) = create_encoder(level, block_sizes[active_block_size].clone(), stdout()) {
          let mut do_restart_encoder = false;
          let mut waited = 0;
          let mut block_max_size = 0;
          let mut unflushed_data = false;
          let mut last_written_between_flush = 0;
          let mut good_blocks = 0;
          loop {
            waited += 1;
            if do_restart_encoder {
               eprintln!("Restarting encoder");
               let result = restart_encoder(level, block_sizes[active_block_size].clone(), encoder);
               if let Ok(newencoder) = result {
                 encoder = newencoder;
                 if let Err(e) = encoder.flush() {
                   eprintln!("Failed! {}", e);
                 }
               } else {
                 eprintln!("Restarting failed");
                 return;
               }
               eprintln!("New block size is {} bytes", block_sizes[active_block_size].get_size());
               do_restart_encoder = false;
               unflushed_data = false;
            }

            match if unflushed_data
                    { data_receiver.try_recv().map_or(None, |r| Some(r)) }
                  else
                    { data_receiver.recv_timeout(time::Duration::from_millis(500)).map_or(None, |r| Some(r)) } {
              Some(WorkerPackage::Data(recvd, len)) => {
                if len == 0 { encoder.finish(); break }
                waited = 0;
                let mut written = 0;
                while written != len {
                  if let Ok(last_write) = encoder.write(&recvd[written..len-written]) {
                    written += last_write;
                  } else {
                    eprintln!("Write failed");
                  }
                }
                unflushed_data = true;
                good_blocks += 1;
                if good_blocks == 100 && active_block_size != 0 {
                  active_block_size = 0;
                  do_restart_encoder = true;
                }
              },
              Some(WorkerPackage::SetLevel(newlevel)) => {
                eprintln!("Changing from level {} to level {}", level, newlevel);
                level = newlevel;
                do_restart_encoder = true;
              },
              Some(WorkerPackage::SetBlockSize(new_blocksize)) => {
                if new_blocksize < block_sizes.len() {
                  active_block_size = new_blocksize;
                  do_restart_encoder = true;
                }
              },
              _ => {
                if unflushed_data {
                  good_blocks = 0;
                  encoder.flush();
                  unflushed_data = false;
                } 
              }
            }
          }
        } else {
          return;
        }
    });

    loop {
        let mut buf = vec![0; buffer_size];
        //match return_receiver.recv() {
        //    Ok(mut buf) => {
              let len = my_stdin.read(&mut buf[..])?;
              //let new_buf = my_stdin.fill_buf()?;
              //let msg = Vec::from(new_buf);
              if let Err(e) = data_sender.send(WorkerPackage::Data(buf, len)) {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
              }
              if len == 0 { break }

            //},
            //Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
        //}
    }
    if let Err(e) = child.join() {
    }
    Ok(())
}
