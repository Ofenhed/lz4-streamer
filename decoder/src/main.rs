extern crate lz4;
use std::{thread,time};
use std::io::{stdout, stdin, Read, Write, Result as IOResult, BufReader, BufRead, copy};
use std::env::args;
use std::fs::File;

// use zstd::stream::write::{Encoder, AutoFinishEncoder};
use lz4::Decoder;
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver, SyncSender, SendError};
use std::os::unix::io::{FromRawFd, AsRawFd};

enum CommonSender<T> {
  Sync(SyncSender<T>),
  Free(Sender<T>),
}

fn createSender<'a, T: 'a>(sender: CommonSender<T>) -> Box<dyn FnMut(T) -> Result<(), SendError<T>> + 'a> {
   match sender {
     CommonSender::Sync(s) => Box::new(move |x| s.send(x)),
     CommonSender::Free(s) => Box::new(move |x| s.send(x)),
   }
}

fn main() -> IOResult<()> {
    let my_stdin = stdin();
    let mut my_stdin = my_stdin.lock();

    let num_buffers = 1024;
    let mut args = args();
    let program = args.next();
    let arg = args.next();
    let (mut data_sender, data_receiver) = {
      let (data_sender_sync, data_receiver_sync): (SyncSender<(Vec<u8>, usize)>, Receiver<(Vec<u8>, usize)>) = sync_channel(num_buffers);
      let (data_sender, data_receiver): (Sender<(Vec<u8>, usize)>, Receiver<(Vec<u8>, usize)>) = channel();
      if let Some(is_unsafe) = arg {
          if is_unsafe == "--unsafe" {
            (createSender(CommonSender::Free(data_sender)), data_receiver)
          } else {
            panic!("Usage: {} [{}]\nUnsafe means that the program may fill all up available memory if the output buffer is slower than incoming data.")
          }
      } else {
        (createSender(CommonSender::Sync(data_sender_sync)), data_receiver_sync)
      }
    };

    let child = thread::spawn(move|| {
        let mut is_flushed = true;
        let mut stdout = stdout();
        loop {
            if let Some((recvd, len)) = if !is_flushed
                    { data_receiver.try_recv().map_or(None, |r| Some(r)) }
                  else
                    { data_receiver.recv_timeout(time::Duration::from_millis(500)).map_or(None, |r| Some(r)) }
            {
              if len == 0 { break }
              let mut written = 0;
              while written != len {
                if let Ok(last_write) = stdout.write(&recvd[written..len-written]) {
                  written += last_write;
                } else {
                  eprintln!("Write failed");
                }
              }
              is_flushed = false;
            } else if !is_flushed {
              stdout.flush().unwrap();
              is_flushed = true;
            }
        }
    });
    
    let mut input = my_stdin;
    let mut instant_failed = 0;
    loop {
      let mut decoder = lz4::Decoder::new(input)?;
      let mut first_decode = true;
      loop {
        let mut buf = vec![0; 128*1024];
        if let Ok(len) = decoder.read(&mut buf[..]) {
          if len == 0 { break }
          first_decode = false;
          data_sender((buf, len));
        } else { break }
      }
      let (tmp, _) = decoder.finish();
      input = tmp;
      if first_decode { instant_failed += 1 }
      if instant_failed == 100 { break }
    }
    Ok(())
}
