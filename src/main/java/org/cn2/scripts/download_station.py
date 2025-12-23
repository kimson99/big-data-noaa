import os
import re
import subprocess
import urllib.request
import ssl

# 1. Cấu hình
BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/"
LIST_FILE = "file_station_list.txt"
# Đường dẫn lưu file tương đối từ thư mục scripts
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../resources/data/station"))
MAX_FILES = 500

def download_setup():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"[*] Đã tạo thư mục: {OUTPUT_DIR}")

    print(f"[*] Đang lấy danh sách file từ {BASE_URL}...")
    
    try:
        # Bỏ qua kiểm tra SSL nếu mạng gặp lỗi chứng chỉ
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(BASE_URL, context=context) as response:
            html = response.read().decode('utf-8')
            links = re.findall(r'href="([^"]+\.csv\.gz)"', html)
            
            selected_links = links[:MAX_FILES]
            with open(LIST_FILE, "w") as f:
                for link in selected_links:
                    f.write(BASE_URL + link + "\n")
            
            print(f"[*] Thành công! Tìm thấy {len(selected_links)} file.")
            return True
    except Exception as e:
        print(f"[!] Lỗi khi lấy danh sách: {e}")
        return False

def run_aria2c():
    print("[*] Đang bắt đầu tải bằng aria2c...")
    # Kiểm tra xem aria2c.exe có trong thư mục hiện tại hoặc PATH không
    try:
        cmd = [
            "aria2c", "-i", LIST_FILE,
            "-x16", "-s16", "-j4",
            "-d", OUTPUT_DIR,
            "--continue=true",
            "--file-allocation=none"
        ]
        subprocess.run(cmd, check=True)
        print("[*] Tải dữ liệu hoàn tất!")
    except FileNotFoundError:
        print("[!] Không tìm thấy aria2c.exe. Hãy đảm bảo bạn đã để file aria2c.exe cùng thư mục này.")
    except Exception as e:
        print(f"[!] Lỗi khi chạy aria2c: {e}")

if __name__ == "__main__":
    if download_setup():
        run_aria2c()
        if os.path.exists(LIST_FILE):
            os.remove(LIST_FILE)