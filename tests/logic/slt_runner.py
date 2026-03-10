import socket
import struct
import sys
import os
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
        
        # PostgreSQL Startup Message
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
                status = body.decode('utf-8').strip('\0')
            elif type_char == 'E':
                status = "ERROR"
            elif type_char == 'Z':
                break

        return rows, status

def normalize_value(val):
    if val is None:
        return "NULL"
    return str(val)

def compare_values(actual, expected, col_type):
    if expected == "NULL":
        return actual is None
    if actual is None:
        return expected == "NULL"
    
    if col_type == 'R': # Float/Real
        try:
            return math.isclose(float(actual), float(expected), rel_tol=1e-6)
        except (ValueError, TypeError):
            return str(actual) == str(expected)
    
    return str(actual) == str(expected)

def run_slt(file_path, port):
    client = CloudSQLClient(port=port)
    try:
        client.connect()
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        return False

    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return False

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

        start_line = line_idx + 1

        if line.startswith('statement'):
            parts = line.split()
            expected_status = parts[1] # ok or error
            
            sql_lines = []
            line_idx += 1
            while line_idx < len(lines) and lines[line_idx].strip():
                sql_lines.append(lines[line_idx].strip())
                line_idx += 1
            
            sql = " ".join(sql_lines)
            total_tests += 1
            _, actual_status = client.query(sql)
            
            is_error = actual_status == "ERROR"
            matches = (expected_status == "error" and is_error) or (expected_status == "ok" and not is_error)
            
            if not matches:
                print(f"FAILURE at {file_path}:{start_line}")
                print(f"  SQL: {sql}")
                print(f"  Expected status: {expected_status}, got: {actual_status}")
                failed_tests += 1

        elif line.startswith('query'):
            # query <types> [sort]
            parts = line.split()
            types = parts[1]
            sort_mode = parts[2] if len(parts) > 2 else None
            
            if sort_mode and sort_mode not in ['rowsort', 'valuesort']:
                print(f"ERROR at {file_path}:{start_line}: Unsupported sort mode '{sort_mode}'")
                sys.exit(1)
            
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
                print(f"FAILURE at {file_path}:{start_line}")
                print(f"  SQL: {sql}")
                print("  Query failed with ERROR status")
                failed_tests += 1
                continue

            # Apply SLT sort modes
            if sort_mode == 'rowsort':
                actual_rows.sort()
                expected_rows.sort()
            elif sort_mode == 'valuesort':
                # Valuesort sorts every individual value in the result set
                actual_vals = sorted([normalize_value(v) for row in actual_rows for v in row])
                expected_vals = sorted([v for row in expected_rows for v in row])
                actual_rows = [[v] for v in actual_vals]
                expected_rows = [[v] for v in expected_vals]
                # Update types to all be 'T' since we flattened everything to strings for valuesort
                types = 'T' * len(actual_vals)

            # Compare row counts
            if len(actual_rows) != len(expected_rows):
                print(f"FAILURE at {file_path}:{start_line}")
                print(f"  SQL: {sql}")
                print(f"  Expected {len(expected_rows)} rows, got {len(actual_rows)}")
                print(f"  Actual rows: {actual_rows}")
                failed_tests += 1
                continue

            # Compare cell by cell
            for i in range(len(actual_rows)):
                if len(actual_rows[i]) != len(expected_rows[i]):
                    print(f"FAILURE at {file_path}:{start_line}, row {i}")
                    print(f"  Expected {len(expected_rows[i])} columns, got {len(actual_rows[i])}")
                    failed_tests += 1
                    break
                
                match = True
                for j in range(len(actual_rows[i])):
                    act = actual_rows[i][j]
                    exp = expected_rows[i][j]
                    col_type = types[j] if j < len(types) else 'T'
                    
                    if not compare_values(act, exp, col_type):
                        print(f"FAILURE at {file_path}:{start_line}, row {i} col {j}")
                        print(f"  SQL: {sql}")
                        print(f"  Expected '{exp}', got '{normalize_value(act)}'")
                        print(f"  Full row: {[normalize_value(v) for v in actual_rows[i]]}")
                        match = False
                        break
                if not match:
                    failed_tests += 1
                    break
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
