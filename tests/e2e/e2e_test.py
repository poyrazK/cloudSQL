import socket
import struct
import time

PROTOCOL_VERSION_3 = 196608

class CloudSQLClient:
    def __init__(self, host='127.0.0.1', port=5432):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        print(f"Connecting to {self.host}:{self.port}...")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(2.0)
        self.sock.connect((self.host, self.port))
        
        # PostgreSQL Startup packet is just Int32 Length, Int32 Protocol
        length = 8
        packet = struct.pack('!II', length, PROTOCOL_VERSION_3)
        print(f"Sending startup packet: {packet}")
        self.sock.sendall(packet)

        # Wait for AuthOK 'R' and ReadyForQuery 'Z'
        print("Waiting for R...")
        try:
            r_type = self.sock.recv(1)
            print(f"Got: {r_type}")
            if r_type != b'R':
                raise Exception(f"Expected AuthOK 'R', got {r_type}")
            self.sock.recv(8) # length + 4 bytes content
            
            z_type = self.sock.recv(1)
            print(f"Got: {z_type}")
            if z_type != b'Z':
                raise Exception(f"Expected ReadyForQuery 'Z', got {z_type}")
            self.sock.recv(5) # length + 1 byte state
        except Exception as e:
            print(f"Error reading handshake: {e}")
            raise

    def query(self, sql):
        sql_bytes = sql.encode('utf-8') + b'\0'
        # Packet length includes the 4 byte length itself
        length = 4 + len(sql_bytes)
        packet = b'Q' + struct.pack('!I', length) + sql_bytes
        self.sock.sendall(packet)

        rows = []
        columns = []
        status = None

        while True:
            type_byte = self.sock.recv(1)
            if not type_byte:
                break
            
            type_char = type_byte.decode()
            
            len_bytes = self.sock.recv(4)
            if not len_bytes:
                break
            length = struct.unpack('!I', len_bytes)[0]
            
            body = self.sock.recv(length - 4)

            if type_char == 'T':
                # Parse RowDescription
                num_fields = struct.unpack('!h', body[:2])[0]
                idx = 2
                for _ in range(num_fields):
                    null_idx = body.find(b'\0', idx)
                    col_name = body[idx:null_idx].decode('utf-8')
                    columns.append(col_name)
                    idx = null_idx + 1 + 4 + 2 + 4 + 2 + 4 + 2

            elif type_char == 'D':
                # Parse DataRow
                num_cols = struct.unpack('!h', body[:2])[0]
                idx = 2
                row_data = []
                for _ in range(num_cols):
                    col_len = struct.unpack('!I', body[idx:idx+4])[0]
                    idx += 4
                    if col_len == 0xFFFFFFFF: # -1
                        row_data.append(None)
                    else:
                        val = body[idx:idx+col_len].decode('utf-8')
                        row_data.append(val)
                        idx += col_len
                rows.append(row_data)

            elif type_char == 'C':
                # CommandComplete
                status = body[:-1].decode('utf-8')

            elif type_char == 'E':
                status = "ERROR"

            elif type_char == 'Z':
                # ReadyForQuery
                break

        return columns, rows, status

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5432
    time.sleep(1) # wait for server
    try:
        client = CloudSQLClient(port=port)
        client.connect()
        print("Connected successfully!")
        
        print("Testing CREATE TABLE...")
        cols, rows, status = client.query("CREATE TABLE users (id INT, name TEXT, age INT);")
        assert status == "OK", f"Create failed, status: {status}"
        
        print("Testing INSERT...")
        for i in range(1, 4):
            cols, rows, status = client.query(f"INSERT INTO users VALUES ({i}, 'User{i}', {20+i});")
            assert status == "OK", "Insert failed"
            
        print("Testing SELECT...")
        cols, rows, status = client.query("SELECT id, name, age FROM users;")
        assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
        assert rows[0] == ["1", "User1", "21"]
        
        print("Testing WHERE clause...")
        cols, rows, status = client.query("SELECT id, name FROM users WHERE id = 2;")
        assert len(rows) == 1
        assert rows[0] == ["2", "User2"]
        
        print("Testing UPDATE...")
        cols, rows, status = client.query("UPDATE users SET age = 99 WHERE id = 2;")
        assert status == "OK"
        cols, rows, status = client.query("SELECT age FROM users WHERE id = 2;")
        assert rows[0][0] == "99"
        
        print("Testing DELETE...")
        cols, rows, status = client.query("DELETE FROM users WHERE id = 1;")
        assert status == "OK"
        cols, rows, status = client.query("SELECT id FROM users;")
        assert len(rows) == 2, "Row should be deleted"
        
        print("Testing DROP TABLE...")
        cols, rows, status = client.query("DROP TABLE users;")
        assert status == "OK"
        
        print("All E2E checks PASSED.")
    except Exception as e:
        print(f"E2E Test Failed: {e}")
        exit(1)
