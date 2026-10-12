# REPORT — Lab Day 9

> Số liệu lấy từ các lượt chạy thực tế với MSSV `2A202602422`; log, JSON và ảnh được lưu trong `report/`.

| | |
|---|---|
| Họ và tên | Nguyễn Triều Vương |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602422` |
| GitHub | `vuog23` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07774` |

MSSV có chữ A nên không dùng trực tiếp được lệnh `int(MSSV)` của GUIDE. Riêng chọn trip recovery dùng các chữ số `2202602422`: modulo 26 bằng 20, tương ứng `trip-07774` trong danh sách đã sắp xếp. Cách chuyển đổi này đã được thống nhất; dataset và mọi thí nghiệm vẫn dùng MSSV đầy đủ `2A202602422`.

## Kết quả cuối

- `dataset_version` của bạn: `33420e167f7d` (đối chiếu `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **685 / 179 / 155**, tổng **1.019**; số trip tương ứng **18 / 4 / 4**.
- Bronze: 3.021 dòng A, 3.234 dòng B, 26 trip, 2 video. Silver: 1.246 frame lấy mẫu ở 2 fps. Chín unit test đều đạt; warehouse có 6 mart và 1.019 dòng gallery.
- `report/verify.txt` lưu đầu ra verify: **27/27 checks passed**. Lượt cuối chạy từ lake trống, cả năm bước đạt và `verify_all_passed = true`.
- Ảnh trong `report/screenshots/`: `airflow_graph.jpg`, `minio_lake.jpg`, `metabase_dashboard.jpg`, `metabase_personal_sql.jpg`, `fail_task_log.jpg`, `airflow_clear_scope.jpg`, `airflow_retry_graph.jpg`.

| Drive | Train: frame / trip | Val: frame / trip | Test: frame / trip |
|---|---:|---:|---:|
| A | 293 / 9 | 75 / 2 | 68 / 2 |
| B | 392 / 9 | 104 / 2 | 87 / 2 |

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

A dùng `ts_utc` dạng ISO UTC, `speed_mps` theo m/s, `heading_deg`, `lat/lon`, `acc_x/acc_y`, `gas/brake` dạng tỷ lệ. B dùng `timestamp_ms` dạng epoch milliseconds, `spd_kmh` theo km/h, `hdg`, `latitude/longitude`, `ax/ay`, `throttle_pct/brake_pct` theo phần trăm. Đồng hồ B còn chạy nhanh 1.500 ms, A không có offset. Ví dụ dòng đầu A có `1.213` m/s, còn B có `55.11` km/h.

Mỗi bảng Bronze bảo toàn schema và giá trị thô, kể cả ô trống; giữ dạng chữ cho phép làm sạch lại với luật mới mà không mất dữ liệu nguồn. Bronze nhận 3.021 dòng A, 3.234 dòng B, 26 trip và 2 video. Ingest A lần hai báo `already present`; `bronze.raw_telemetry_a` chỉ có version 0, một commit. Video được nhận diện bằng SHA256. Bằng chứng: `report/execution/step1.txt` và `bronze_history.json`.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging A đổi ISO sang `timestamptz`; B trừ offset 1.500 ms trước khi đổi epoch ms sang UTC, chia tốc độ cho 3,6 và bàn đạp cho 100. Các cột đổi về cùng tên rồi union theo tên. Dòng đầu B sau chỉnh là `2025-10-08 14:30:00.009+00:00`, sau đầu video 9 ms thay vì 1.509 ms. Tốc độ cực đại quan sát xấp xỉ 71 km/h ở A, 100 km/h ở B; cả 7 kiểm tra module 2 đạt.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng rejects | Dòng sạch |
|---|---:|---:|---:|---:|---:|
| A | 20 | 30 | 3 | 53 | 2.968 |
| B | 20 | 30 | 3 | 53 | 3.181 |

`2.968 + 53 = 3.021` và `3.181 + 53 = 3.234`: mọi dòng nhận về đều được giải trình. Rejects giữ dữ liệu và lý do để điều tra nguồn, sửa luật và tái xử lý. Xóa im lặng sẽ mất khả năng này. Bảng trên khớp `run_summary.json` và log bước 2.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` là `warn` vì mất tín hiệu là lỗi đã biết: cần đo/điều tra nhưng không phải mọi frame đều hỏng. Frame vượt dung sai đồng bộ vẫn bị loại khỏi Gold. `in_range` là `error` vì tốc độ ngoài 0–70 m/s không được phép đi tiếp. Lượt thường có `PASS=6 WARN=1 ERROR=0`.

Trong `break_gate.json`, nới luật làm sạch lên 1.000 m/s làm test `in_range_silver_telemetry_speed_mps__70__0` bắt **6 dòng**, kết quả `PASS=5 WARN=1 ERROR=1`. Khôi phục luật trả lại `PASS=6 WARN=1 ERROR=0`. Cổng test kiểm tra độc lập với luật làm sạch; nếu luật bị nới sai, task test lỗi sẽ chặn Gold/publish phía sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` thử `trip-00003`, có **60 frame**. Toàn bảng có **1.246 dòng** trước và sau hai lần chạy lại; fingerprint đều là `6c7480dfde7e`. Bảng thử append hai lần có **120 dòng**, gấp đôi số frame của trip.

Delta overwrite với predicate `trip_id` thay đúng dữ liệu trip bằng một commit transaction; độc giả nhìn snapshot đã commit, không thấy khoảng trống giữa xóa và ghi. Các trip khác không bị nhân đôi hoặc bị xóa. Nếu lỗi trước commit, snapshot trước đó là điểm phục hồi; retry chỉ cần xử lý lại partition lỗi. Fingerprint ở thí nghiệm tính nội dung xử lý frame, không tính `_processed_at` vốn đổi mỗi lượt.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có brightness dưới **12**, thường là mất hình; khung tối có brightness từ **12 đến dưới 40**, vẫn có thể chứa cảnh trong bóng râm/đường hầm. Silver giữ frame và gắn cờ. Gold bỏ đen, mờ, gần trùng hoặc mất đồng bộ, nhưng không loại chỉ vì `is_dark`; tôi đồng ý vì bỏ mọi cảnh tối sẽ làm mất điều kiện vận hành quan trọng. A có 6 frame đen, 4 frame tối; B có 4 frame đen, 61 frame tối. Trong Gold giữ **4/4** frame tối A và **58/61** frame tối B: ba frame tối B bị loại vì cờ chất lượng khác. Nguồn: `final_snapshot.json`.

Tôi giữ ngưỡng **2** cho lượt nộp: ngưỡng 0 bỏ sót nhiều frame gần giống nhau, còn 6 coi **607/1.246 frame** là trùng, dễ mất cảnh thay đổi thật. Xe chậm/camera đứng hình làm ảnh liền nhau ít đổi nên Hamming thường nhỏ; xe nhanh có thể thay cảnh nhanh hơn. dHash là heuristic, cần đối chiếu ảnh và tốc độ để chọn ngưỡng, không diễn giải số bit như chất lượng ngữ nghĩa. Số liệu lấy từ `threshold.json`.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` dùng **1.019 frame thuộc 26 trip**. Chia theo frame làm **26 trip** xuất hiện ở nhiều split; cả **26 trip** có frame ở train lẫn test. Gold chia theo trip cho **0 trip** bị chia cắt.

Frame cùng trip chia sẻ đường, ánh sáng và bối cảnh, nên mô hình có thể thấy cảnh gần như trùng với test trong lúc train. Điểm kiểm thử vì vậy có thể đẹp mà không phản ánh khả năng chạy trên chuyến mới. Giữ mỗi trip trong một split giảm dạng rò rỉ này; chưa chứng minh các trip khác nhau hoàn toàn độc lập về địa điểm/cảnh quay.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Export đầu và export lại đều cho **`33420e167f7d`**, với **685/179/155 frame**. Code băm danh sách `frame_id|split` đã sắp xếp, lấy 12 ký tự đầu SHA256; giờ chạy không nằm trong đầu vào băm. Retry có thể tạo commit Delta mới nhưng vẫn giữ dataset_version nếu thành phần frame/split không đổi.

MSSV đầy đủ `2A202602422` được nối với trip_id trong `md5` để xếp thứ tự trip trong từng drive. Đổi MSSV có thể đổi trip vào split, số frame và version; đây không phải bảo đảm version duy nhất theo từng người. Băm hiện tại bao phủ thành phần/split, chưa bao phủ toàn bộ pixel hay mọi giá trị telemetry, nên tái lập chắc chắn vẫn cần checksum nguồn, manifest và phiên bản code.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Dataset lấy từ `drive_a.mp4`, `drive_b.mp4` của gói lab L2D-v3 camera front_left, qua telemetry A/B và metadata trip. `manifest.json` ghi version của `bronze.raw_telemetry_a`, `bronze.raw_telemetry_b`, `bronze.trips_raw`, `bronze.video_files`, `silver.frames`, `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry`, cùng SHA256 của hai video. Bản manifest lượt cuối được lưu nguyên nội dung ở `report/evidence/dataset_manifest.json`.

Sau ba tháng cần giữ video/CSV đúng checksum trong `data/MANIFEST.json`, MSSV đầy đủ, code và cấu hình lấy mẫu/chất lượng/split, cùng snapshot Delta được chỉ trong lineage nếu dùng time travel. `dataset_version` đối chiếu frame/split; `manifest_sha256` đối chiếu manifest Parquet. Chỉ số version mà thiếu file log/data tương ứng không đủ để khôi phục.

| Bảng upstream trong manifest lượt cuối | Delta version |
|---|---:|
| bronze.raw_telemetry_a | 0 |
| bronze.raw_telemetry_b | 0 |
| bronze.trips_raw | 0 |
| bronze.video_files | 1 |
| silver.frames | 25 |
| silver.telemetry | 0 |
| silver.frames_flagged | 0 |
| silver.frame_telemetry | 0 |

SHA256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. SHA256 manifest Parquet: `babd3ffd1f4ed55fed30f65c26bb38168adabdc6fd0b67855dc4a8ae364772f5`. Các giá trị này lấy trực tiếp từ `dataset_manifest.json` của lượt cuối.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Lượt `lab-recovery-trip-07774` có **1 task đỏ** (`silver_frames`, map index **20**), **7 task upstream_failed**, và **25 extract khác** thành công. Log có `RuntimeError: simulated crash while extracting trip-07774; run it again`. Trước recovery có **1.212 frame**, thiếu đúng **34 frame** của trip này.

Trong UI chọn riêng map index 20 rồi `Clear task`, giữ Downstream mặc định. Preview có **8 instance**: partition lỗi và 7 task phụ thuộc bị chặn; không gồm 25 partition đã thành công hay các task Bronze. Cần giải phóng downstream để chúng chạy sau retry; đây không phải clear cả DAG. `airflow_clear_scope.jpg` ghi rõ danh sách.

Sau recovery, cả **37 task instance** thành công. Chỉ `silver_frames[20]` có try_number **2**, 25 extract còn lại giữ **1**. Silver trở lại **1.246 dòng**, fingerprint **`6c7480dfde7e`**, không nhân đôi; Gold có **1.019 frame**, version vẫn **`33420e167f7d`**. Verify đạt **27/27**. Bằng chứng: `airflow_failure_tasks.json`, `airflow_recovery_tasks.json`, `recovery_before.json`, `recovery_snapshot.json`, ảnh log/clear/graph, và `report/execution/airflow_recovery_verify.txt`. Công tắc `LAB_FAIL_ONCE` được bỏ sau thí nghiệm.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Đây là dự đoán kiến trúc, chưa phải benchmark 500 xe. Nếu thời lượng mỗi drive không đổi, lượng dữ liệu tăng khoảng 250 lần so với 2 drive hiện tại; nếu xe ghi cả ngày thì tăng cao hơn.

1. **FFmpeg và executor một máy**: mỗi trip tải lại toàn video; DAG giới hạn một extract đang hoạt động trong mỗi run. Nên dùng worker phân tán theo trip, cache/cắt video theo segment, hàng đợi và quota; GPU decode chỉ sau khi đo lợi ích.
2. **DuckDB/work file tập trung**: một file local và dựng/publish toàn bảng sẽ bị giới hạn tài nguyên/I/O. Nên xử lý incremental theo drive/ngày/trip, rồi chuyển phần nặng sang engine lakehouse phân tán như Spark khi số liệu đo yêu cầu.
3. **MinIO một node và nhiều JPEG nhỏ**: các worker cùng đọc/ghi qua một dịch vụ, ảnh còn được nhân bản vào Gold. Nên dùng object storage có khả năng mở rộng, batching/manifest, reference ảnh và chính sách vòng đời. Postgres cũng cần mart tổng hợp/index thay vì đưa mọi frame vào dashboard.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Trip nào mất tỷ lệ frame lớn nhất?** Câu hỏi đã thêm vào dashboard `http://localhost:3000/dashboard/2`; số liệu lưu trong `metabase_questions.json`.

```sql
select drive_id, trip_id, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as dropped_pct,
       avg_speed_kmh, lighting, split
from marts.mart_trip_summary
order by dropped_pct desc, trip_id;
```

`trip-04622` của A đứng đầu: **60 frame, giữ 26, loại 34 (56,7%)**; `trip-03904`: **22 frame, giữ 11 (50%)**. Trip recovery `trip-07774`: **34 frame, giữ 21, loại 13 (38,2%)**. Tổng trừ giữ tránh cộng các cờ chồng lấp. Đây là danh sách ưu tiên kiểm tra video/cảm biến; một mình bảng này chưa xác định nguyên nhân của từng frame bị loại.

### Bonus · Ánh sáng có được đại diện trong mọi split?

```sql
select split, lighting, count(*) as trips, sum(frames_kept) as kept_frames,
       round(sum(frames_kept) * 100.0 /
             nullif(sum(sum(frames_kept)) over (partition by split), 0), 1) as share_pct
from marts.mart_trip_summary
group by split, lighting
order by split, lighting;
```

Train **18 trip/685 frame**, val **4/179**, test **4/155**, tất cả `lighting = Day`, **100%** mỗi split. Không có trip gắn nhãn Night để đánh giá chạy ban đêm. 61 frame tối của B không đồng nghĩa 61 frame ban đêm: bóng râm/đường hầm và metadata Day có thể cùng tồn tại. Đây là một thiếu sót về coverage trước khi dùng dataset đánh giá môi trường khác. Câu hỏi thứ hai đã thêm thật vào dashboard, có ảnh/kết quả SQL; cấu hình lượt nộp chính giữ nguyên.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

A có nhiều frame gần trùng (149) dù các test vẫn đạt, còn B có nhiều frame tối nhưng chúng không tự động là lỗi. Kiểm tra trip mất hơn nửa số frame có ích hơn chỉ nhìn task xanh. Tôi còn muốn xem ảnh bị loại ở các ngưỡng dHash để đánh giá mất cảnh hiếm. Pipeline chưa kiểm tra GPS nhảy bất hợp lý, heading sai nhưng tốc độ hợp lệ, hoặc hai trip khác nhau quay cùng cảnh. Băm dataset hiện tại chưa bao phủ toàn bộ pixel/telemetry. Đây là hướng kiểm tra thêm, không phải lỗi đã được thí nghiệm xác nhận trong bài.
