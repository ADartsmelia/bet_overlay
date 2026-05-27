import sys
import zmq

port = int(sys.argv[1])
cmd  = sys.argv[2]

ctx  = zmq.Context()
sock = ctx.socket(zmq.REQ)
sock.setsockopt(zmq.LINGER,   0)
sock.setsockopt(zmq.RCVTIMEO, 2000)
sock.setsockopt(zmq.SNDTIMEO, 2000)
sock.connect(f"tcp://127.0.0.1:{port}")
sock.send_string(cmd)
sock.recv_string()
sys.exit(0)
