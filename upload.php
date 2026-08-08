<?php
// Bật hiển thị lỗi để dễ debug trong quá trình setup (có thể tắt đi khi chạy thật)
ini_set('display_errors', 0);
error_reporting(E_ALL);

header('Access-Control-Allow-Origin: *');

// --- CẤU HÌNH ---
// Thư mục lưu file tải lên (Đảm bảo aapanel đã cấp quyền ghi 755 hoặc 777 cho thư mục này)
$uploadDir = 'downloads/'; 

// Tên miền của anh (ví dụ: https://domaincuatao.com/downloads/)
$baseUrl = 'http://file.timhieuluat.com/downloads/'; 

// Khóa bảo mật (Secret Key) khớp với cấu hình trong tool.py
$secretKey = 'Hotromt2012!';
// ---------------

// Tự động tạo thư mục nếu chưa có
if (!is_dir($uploadDir)) {
    mkdir($uploadDir, 0755, true);
}

// Xử lý POST request từ Tool Python
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    header('Content-Type: application/json; charset=utf-8');
    
    // Kiểm tra Secret Key
    if (!isset($_POST['secret']) || $_POST['secret'] !== $secretKey) {
        echo json_encode(['status' => 'error', 'message' => 'Sai Secret Key! Không có quyền Upload.']);
        exit;
    }

    // Kiểm tra có file gửi lên không
    if (!isset($_FILES['file']) || $_FILES['file']['error'] !== UPLOAD_ERR_OK) {
        echo json_encode(['status' => 'error', 'message' => 'Không nhận được file hoặc file bị lỗi.']);
        exit;
    }

    $file = $_FILES['file'];
    
    // Làm sạch tên file để tránh lỗi Unicode hoặc ký tự đặc biệt
    $originalName = basename($file['name']);
    $safeName = preg_replace("/[^a-zA-Z0-9.\-_]/", "", $originalName); 
    if(empty($safeName)) {
        $safeName = 'document_'.time().'.doc';
    }
    
    // Gắn thêm timestamp để không bị đè tên trùng
    $finalName = time() . '_' . $safeName;
    $targetPath = $uploadDir . $finalName;

    // Tiến hành lưu file
    if (move_uploaded_file($file['tmp_name'], $targetPath)) {
        $fileUrl = $baseUrl . $finalName;
        echo json_encode([
            'status' => 'success', 
            'url' => $fileUrl,
            'message' => 'Đã lưu file thành công'
        ]);
    } else {
        echo json_encode(['status' => 'error', 'message' => 'Không thể di chuyển file vào thư mục đích. (Kiểm tra phân quyền)']);
    }
    exit;
}
?>

<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>Test API Upload Tự Động</title>
    <style>
        body { font-family: Arial, sans-serif; padding: 20px; background: #f4f4f4; }
        .box { background: #fff; padding: 20px; border-radius: 8px; max-width: 500px; margin: auto; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        input[type="text"], input[type="file"] { width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
        button { background: #28a745; color: #fff; border: none; padding: 10px 15px; cursor: pointer; border-radius: 4px; font-weight: bold; }
        button:hover { background: #218838; }
        #result { margin-top: 15px; padding: 10px; border-radius: 4px; }
        .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    </style>
</head>
<body>
    <div class="box">
        <h2>Kiểm tra chức năng Upload</h2>
        <form id="uploadForm" enctype="multipart/form-data">
            <label>Secret Key:</label>
            <input type="text" name="secret" id="secret" value="timhieuluat_upload_secret" required>
            
            <label>Chọn file tải lên:</label>
            <input type="file" name="file" id="file" required>
            
            <button type="submit">Tải Lên Thử</button>
        </form>

        <div id="result" style="display:none;"></div>
    </div>

    <script>
        document.getElementById('uploadForm').addEventListener('submit', function(e) {
            e.preventDefault();
            var resultDiv = document.getElementById('result');
            resultDiv.style.display = 'none';
            resultDiv.className = '';
            
            var formData = new FormData(this);

            fetch('', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                resultDiv.style.display = 'block';
                if (data.status === 'success') {
                    resultDiv.className = 'success';
                    resultDiv.innerHTML = '<strong>Thành công!</strong> Link file: <br><a href="' + data.url + '" target="_blank">' + data.url + '</a>';
                } else {
                    resultDiv.className = 'error';
                    resultDiv.innerHTML = '<strong>Lỗi:</strong> ' + data.message;
                }
            })
            .catch(error => {
                resultDiv.style.display = 'block';
                resultDiv.className = 'error';
                resultDiv.innerHTML = '<strong>Lỗi kết nối:</strong> ' + error.message;
            });
        });
    </script>
</body>
</html>
