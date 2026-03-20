from backend.security.crypto import decrypt_value


class SecureRow(dict):

    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._values = list(values)

    # supports row[0]
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)

    # optional helper
    def as_tuple(self):
        return tuple(self._values)


def auto_decrypt_row(row):

    for k, v in row.items():

        if isinstance(v, str) and v.startswith("gAAAAA"):
            try:
                row[k] = decrypt_value(v)
            except Exception:
                import traceback; traceback.print_exc()
                print('Exception caught', flush=True)

    return row

def fetchone_secure(cur):

    row = cur.fetchone()
    if not row:
        return None

    columns = [c[0] for c in cur.description]

    secure_row = SecureRow(columns, row)

    return auto_decrypt_row(secure_row)


def fetchall_secure(cur):

    rows = cur.fetchall()
    columns = [c[0] for c in cur.description]

    result = []

    for r in rows:
        secure_row = SecureRow(columns, r)
        result.append(auto_decrypt_row(secure_row))

    return result