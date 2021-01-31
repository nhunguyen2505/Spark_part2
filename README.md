# Tìm hiểu Spark
## Tính năng của Spark
Spark có các đặc trưng sau:
 - Tốc độ: Spark có thể chạy trên cụm Hadoop và có thể chạy nhanh 100 lần khi chạy trên bộ nhớ RAM, và nhanh hơn 10 lần khi chạy trên ổ cứng.Bằng việc giảm số thao tác đọc ghi lên đĩa cứng. Nó lưu trưc trực tiếp dữ liệu được xử lý lên bộ nhớ.
 - Hỗ trọ đa ngôn ngữ: Spark cung cấp các API có sẵn cho các ngôn ngữ Java, Scala, hoặc Python, bạn có thể viết các ứng dụng bằng nhiều các ngôn ngữ khác nhau. Spark đi kèm 80 truy vấn tương tác mức cao.
 - Phân tích nâng cao: Spark không chỉ hỗ trợ ‘Map’ và ‘Reduce’. Nó còn hỗ trợ truy vấn SQL, xử lý theo Stream, học máy, và các thuật toán đồ  (Graph).

Các thành phần của Spark:
<p align = "center"> <img src = https://cdn.noron.vn/2018/10/17/445a8489bc7387575b3ea580a127e458.png?w=600>
 
 - Spark Core: Spark Core là thành phần cốt lõi thực thi cho tác vụ cơ bản làm nền tảng cho các chức năng khác. Nó cung cấp khả năng tính toán trên bộ nhớ và datase trong bộ nhớ hệ thống lưu trữ ngoài.
 - Spark SQL: Là một thành phần nằm trên Spark Core nó cung cấp một sự ảo hóa mới cho dữ liệu là SchemaRDD, hỗ trợ các dữ liệu có cấu trúc và bán cấu trúc. • Spark Streaming: Cho phép thực hiện phân tích xử lý trực tuyến xử lý theo lô.
 - MLlib (Machine Learning Library): MLlib là một nền tảng học máy phân tán bên trên Spark do kiến trúc phân tán dựa trên bộ nhớ. Theo các so sánh benchmark Spark MLlib nhanh hơn chín lần so với phiên bản chạy trên Hadoop (Apache Mahout).
 - GrapX: Grapx là nền tảng xử lý đồ thị dựa trên Spark. Nó cung cấp các Api để diễn tả các tính toán trong đồ thị bằng cách sử dụng Pregel Api.

## Spark RDD (Resilient Distributed Datasets)
### Khái niệm
Resilient Distributed Datasets (RDD) là một cấu trúc dữ liệu cơ bản của Spark. Nó là một tập hợp bất biến phân tán của một đối tượng. Mỗi dataset trong RDD được chia ra thành nhiều phần vùng logical. Có thể được tính toán trên các node khác nhau của một cụm máy chủ (cluster). RDDs có thể chứa bất kỳ kiểu dữ liệu nào của Python, Java, hoặc đối tượng Scala, bao gồm các kiểu dữ liệu do người dùng định nghĩa.

Thông thường, RDD chỉ cho phép đọc, phân mục tập hợp của các bản ghi. RDDs có thể được tạo ra qua điều khiển xác định trên dữ liệu trong bộ nhớ hoặc RDDs, RDD là một tập hợp có khả năng chịu lỗi mỗi thành phần có thể được tính toán song song.

Có hai cách để tạo RDDs:
 - Tạo từ một tập hợp dữ liệu có sẵn trong ngôn ngữ sử dụng như Java, Python, Scala.
 - Lấy từ dataset hệ thống lưu trữ bên ngoài như HDFS, Hbase hoặc các cơ sở dữ liệu quan hệ.
### Thực thi trên Map-Reduce
MapReduce được áp dụng rộng rãi để xử lý và tạo các bộ dữ liệu lớn với thuật toán xử lý phân tán song song trên một cụm. Nó cho phép người dùng viết các tính toán song song, sử dụng một tập hợp các toán tử cấp cao, mà không phải lo lắng về xử lý công việc và khả năng chịu lỗi.

Cả hai ứng dụng Lặp (Iterative) và Tương tác (Interactive) đều yêu cầu chia sẻ truy cập và xử lý dữ liệu nhanh hơn trên các công việc song song. Chia sẻ dữ liệu chậm trong Map-Reduce do sao chép tuần tự và tốc độ I/O của ổ đĩa. Về hệ thống lưu trữ, hầu hết các ứng dụng Hadoop, cần dành hơn 90% thời gian để thực hiện các thao tác đọc-ghi HDFS.

Iterative Operation trên MapReduce:

<p align = "center"><img src=laptrinh.vn/uploads/images/gallery/2019-10/scaled-1680-/iterative_operations_on_mapreduce.jpg>
 
Interactive Operations trên MapReduce:

<p align = "center"><img src=laptrinh.vn/uploads/images/gallery/2019-10/scaled-1680-/interactive_operations_on_mapreduce.jpg>
