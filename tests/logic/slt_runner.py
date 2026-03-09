import socket
import struct
import sys
import time
import math

PROTOCOL_VERSION_3 = 196608

class CloudSQLClient:
    def __init__(self, host='127.0.0.1', port=5432):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5.0)
        self.sock.connect((self.host, self.port))
        
        length = 8
        packet = struct.pack('!II', length, PROTOCOL_VERSION_3)
        self.sock.sendall(packet)

        try:
            r_type = self.recv_exactly(1)
            if r_type != b'R':
                raise Exception(f"Expected AuthOK 'R', got {r_type}")
            self.recv_exactly(8)
            
            z_type = self.recv_exactly(1)
            if z_type != b'Z':
                raise Exception(f"Expected ReadyForQuery 'Z', got {z_type}")
            self.recv_exactly(5)
        except Exception as e:
            raise Exception(f"Handshake failed: {e}")

    def recv_exactly(self, n):
        data = b''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def query(self, sql):
        sql_bytes = sql.encode('utf-8') + b'\0'
        length = 4 + len(sql_bytes)
        packet = b'Q' + struct.pack('!I', length) + sql_bytes
        self.sock.sendall(packet)

        rows = []
        status = "OK"

        while True:
            type_byte = self.recv_exactly(1)
            if not type_byte:
                break
            
            type_char = type_byte.decode()
            len_bytes = self.recv_exactly(4)
            if not len_bytes:
                break
            length = struct.unpack('!I', len_bytes)[0]
            body = self.recv_exactly(length - 4)

            if type_char == 'D':
                num_cols = struct.unpack('!h', body[:2])[0]
                idx = 2
                row_data = []
                for _ in range(num_cols):
                    col_len = struct.unpack('!I', body[idx:idx+4])[0]
                    idx += 4
                    if col_len == 0xFFFFFFFF:
                        row_data.append(None)
                    else:
                        val = body[idx:idx+col_len].decode('utf-8')
                        row_data.append(val)
                        idx += col_len
                rows.append(row_data)
            elif type_char == 'C':
                pass # CommandComplete
            elif type_char == 'E':
                status = "ERROR"
            elif type_char == 'Z':
                break

        return rows, status

def run_slt(file_path, port):
    client = CloudSQLClient(port=port)
    client.connect()

    with open(file_path, 'r') as f:
        lines = f.readlines()

    line_idx = 0
    total_tests = 0
    failed_tests = 0

    while line_idx < len(lines):
        line = lines[line_idx].strip()
        if not line or line.startswith('#'):
            line_idx += 1
            continue

        if line.startswith('statement'):
            expected_status = line.split()[1] # ok or error
            sql_lines = []
            line_idx += 1
            while line_idx < len(lines) and lines[line_idx].strip():
                sql_lines.append(lines[line_idx].strip())
                line_idx += 1
            
            sql = " ".join(sql_lines)
            total_tests += 1
            _, actual_status = client.query(sql)
            
            if actual_status.lower() != expected_status.lower():
                print(f"FAILURE at {file_path}:{line_idx}")
                print(f"  SQL: {sql}")
                print(f"  Expected status: {expected_status}, got: {actual_status}")
                failed_tests += 1

        elif line.startswith('query'):
            # query <types> [sort]
            parts = line.split()
            types = parts[1]
            sort_mode = parts[2] if len(parts) > 2 else None
            
            sql_lines = []
            line_idx += 1
            while line_idx < len(lines) and lines[line_idx].strip() != '----':
                sql_lines.append(lines[line_idx].strip())
                line_idx += 1
            
            sql = " ".join(sql_lines)
            line_idx += 1 # skip '----'
            
            expected_rows = []
            while line_idx < len(lines) and lines[line_idx].strip():
                expected_rows.append(lines[line_idx].strip().split())
                line_idx += 1
            
            total_tests += 1
            actual_rows, status = client.query(sql)
            
            if status == "ERROR":
                print(f"FAILURE at {file_path}:{line_idx}")
                print(f"  SQL: {sql}")
                print(f"  Query failed with ERROR status")
                failed_tests += 1
                continue

            # Apply sort mode
            if sort_mode == 'rowsort':
                actual_rows.sort()
                expected_rows.sort()
            elif sort_mode == 'valuesort':
                actual_values = sorted([str(val) if val is not None else "NULL" for row in actual_rows for val in row])
                expected_values = sorted([val for row in expected_rows for val in row])
                actual_rows = [[v] for v in actual_values]
                expected_rows = [[v] for v in expected_values]
            elif sort_mode:
                print(f"ERROR: Unsupported sort mode: {sort_mode}")
                sys.exit(1)

            # Compare results
            if len(actual_rows) != len(expected_rows):
                print(f"FAILURE at {file_path}:{line_idx}")
                print(f"  SQL: {sql}")
                print(f"  Expected {len(expected_rows)} rows, got {len(actual_rows)}")
                failed_tests += 1
                continue

            for i in range(len(actual_rows)):
                if len(actual_rows[i]) != len(expected_rows[i]):
                    print(f"FAILURE at {file_path}:{line_idx}, row {i}")
                    print(f"  Expected {len(expected_rows[i])} columns, got {len(actual_rows[i])}")
                    failed_tests += 1
                    break
                
                match = True
                for j in range(len(actual_rows[i])):
                    act = actual_rows[i][j]
                    exp = expected_rows[i][j]
                    
                    if exp == "NULL" and act is None:
                        continue
                    
                    # Basic numeric normalization for float comparison
                    if types[j] == 'R' and sort_mode != 'valuesort':
                        try:
                            if not math.isclose(float(act), float(exp), rel_tol=1e-6):
                                match = False
                        except:
                            match = False
                    else:
                        if str(act) != str(exp):
                            match = False
                    
                    if not match:
                        print(f"FAILURE at {file_path}:{line_idx}, row {i} col {j}")
                        print(f"  Expected '{exp}', got '{act}'")
                        failed_tests += 1
                        break
                if not match: break

        else:
            line_idx += 1

    print(f"SLT Summary: {total_tests} tests, {failed_tests} failed.")
    return failed_tests == 0

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 slt_runner.py <port> <slt_file>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    file_path = sys.argv[2]
    
    if run_slt(file_path, port):
        sys.exit(0)
    else:
        sys.exit(1)
