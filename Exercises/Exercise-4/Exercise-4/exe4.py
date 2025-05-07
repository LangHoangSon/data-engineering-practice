import os
import glob
import json
import csv
# boto3 không cần thiết cho bài này

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif isinstance(x, list):
            out[name[:-1]] = ','.join(map(str, x))
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def find_json_files(root_dir):
    pattern = os.path.join(root_dir, '**', '*.json')
    return glob.glob(pattern, recursive=True)


def json_to_csv(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, list):
        flat_data = [flatten_json(item) for item in data]
    else:
        flat_data = [flatten_json(data)]

    if not flat_data:
        print(f"⚠️ File không có dữ liệu: {json_path}")
        return

    csv_path = os.path.splitext(json_path)[0] + '.csv'

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
        writer.writeheader()
        writer.writerows(flat_data)

    print(f"✅ Đã chuyển: {json_path} → {csv_path}")


def main():
    root = 'enough_ready'
    json_files = find_json_files(root)

    if not json_files:
        print("❌ Không tìm thấy file JSON nào.")
        return

    for json_file in json_files:
        try:
            json_to_csv(json_file)
        except Exception as e:
            print(f"❌ Lỗi khi xử lý {json_file}: {e}")


if __name__ == "__main__":
    main()
