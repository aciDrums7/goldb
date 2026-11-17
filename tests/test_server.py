"""Integration tests using only the Python standard library."""
import os
import random
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

BIN = str(Path(__file__).resolve().parents[1] / 'goldb')


def encode(*args):
    args = [str(x).encode() if not isinstance(x, bytes) else x for x in args]
    return b'*%d\r\n' % len(args) + b''.join(b'$%d\r\n' % len(x) + x + b'\r\n' for x in args)


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            self.port = s.getsockname()[1]
        self.start()

    def start(self):
        self.log = tempfile.TemporaryFile()
        self.proc = subprocess.Popen([BIN, '--port', str(self.port), '--aof', self.tmp.name + '/data.aof'], stderr=self.log)
        for _ in range(100):
            try:
                self.sock = socket.create_connection(('127.0.0.1', self.port), timeout=1)
                self.file = self.sock.makefile('rb')
                return
            except OSError:
                if self.proc.poll() is not None:
                    self.log.seek(0)
                    self.fail(self.log.read().decode())
                time.sleep(.02)
        self.fail('server did not start')

    def stop(self):
        self.file.close()
        self.sock.close()
        self.proc.terminate()
        self.proc.wait(timeout=5)
        self.log.close()

    def tearDown(self):
        self.stop()
        self.tmp.cleanup()

    def reply(self):
        line = self.file.readline()
        self.assertTrue(line.endswith(b'\r\n'), line)
        if line[:1] == b'$':
            n = int(line[1:])
            if n == -1:
                return None
            value = self.file.read(n)
            self.assertEqual(self.file.read(2), b'\r\n')
            return value
        if line[:1] == b':':
            return int(line[1:])
        return line[:-2]

    def cmd(self, *args):
        self.sock.sendall(encode(*args))
        return self.reply()

    def test_binary_fragmentation_and_pipeline(self):
        request = encode('SET', b'a\x00b', b'v\r\n\x00')
        for byte in request:
            self.sock.sendall(bytes([byte]))
        self.assertEqual(self.reply(), b'+OK')
        self.sock.sendall(encode('GET', b'a\x00b') + encode('DEL', b'a\x00b') + encode('GET', b'a\x00b'))
        self.assertEqual(self.reply(), b'v\r\n\x00')
        self.assertEqual(self.reply(), 1)
        self.assertIsNone(self.reply())

    def test_randomized_dictionary_and_replay(self):
        rng = random.Random(71)
        expected = {}
        for _ in range(2500):
            key = str(rng.randrange(400)).encode()
            if rng.randrange(3):
                value = rng.randbytes(32)
                self.assertEqual(self.cmd('SET', key, value), b'+OK')
                expected[key] = value
            else:
                self.assertEqual(self.cmd('DEL', key), int(key in expected))
                expected.pop(key, None)
        self.stop()
        self.start()
        for i in range(400):
            key = str(i).encode()
            self.assertEqual(self.cmd('GET', key), expected.get(key))

    def test_expiry_survives_replay_without_extension(self):
        self.cmd('SET', 'short', 'v', 'PX', '100')
        self.cmd('SET', 'long', 'v', 'EX', '60')
        self.stop()
        time.sleep(.15)
        self.start()
        self.assertIsNone(self.cmd('GET', 'short'))
        self.assertEqual(self.cmd('GET', 'long'), b'v')
        self.assertEqual(self.cmd('SET', 'long', 'new'), b'+OK')
        self.assertEqual(self.cmd('TTL', 'long'), -1)

    def test_partial_aof_tail_recovery(self):
        self.cmd('SET', 'key', 'value')
        self.stop()
        with open(self.tmp.name + '/data.aof', 'ab') as f:
            f.write(b'*3\r\n$3\r\nSET\r\n$4\r\ntail\r\n$8\r\nab')
        self.start()
        self.assertEqual(self.cmd('GET', 'key'), b'value')
        self.assertIsNone(self.cmd('GET', 'tail'))
        self.cmd('SET', 'after', 'ok')
        self.stop()
        self.start()
        self.assertEqual(self.cmd('GET', 'after'), b'ok')

    def test_output_limit_disconnects_slow_reader(self):
        self.assertEqual(self.cmd('SET', 'large', b'x' * (8 * 1024 * 1024)), b'+OK')
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as slow:
            slow.sendall(encode('GET', 'large') * 40)
            received = 0
            while True:
                try:
                    chunk = slow.recv(1024 * 1024)
                except ConnectionResetError:
                    break
                if not chunk:
                    break
                received += len(chunk)
            self.assertLess(received, 40 * 8 * 1024 * 1024)
        self.assertEqual(self.cmd('PING'), b'+PONG')

    def test_malformed_protocol_and_reconnect(self):
        self.sock.sendall(b'*2\r\n$4\r\nPINGxx')
        self.assertTrue(self.reply().startswith(b'-ERR Protocol error'))
        self.file.close()
        self.sock.close()
        self.sock = socket.create_connection(('127.0.0.1', self.port), timeout=1)
        self.file = self.sock.makefile('rb')
        self.assertEqual(self.cmd('PING'), b'+PONG')
        self.assertTrue(self.cmd('SET', 'key', 'v', 'EX', '-1').startswith(b'-ERR'))
        self.assertIsNone(self.cmd('GET', 'key'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
