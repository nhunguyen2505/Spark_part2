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
<p align = "center"> <img src = https://static.packt-cdn.com/products/9781785280849/graphics/5536cf53-3947-434f-ae2f-3a1338e2dbab.png>

Cả hai ứng dụng Lặp (Iterative) và Tương tác (Interactive) đều yêu cầu chia sẻ truy cập và xử lý dữ liệu nhanh hơn trên các công việc song song. Chia sẻ dữ liệu chậm trong Map-Reduce do sao chép tuần tự và tốc độ I/O của ổ đĩa. Về hệ thống lưu trữ, hầu hết các ứng dụng Hadoop, cần dành hơn 90% thời gian để thực hiện các thao tác đọc-ghi HDFS.

### Thực thi trên Spark RDD
Để khắc phục được vấn đề về MapRedure, các nhà nghiên cứu đã phát triển một framework chuyên biệt gọi là Apache Spark. Ý tưởng chính của Spark là Resilient Distributed Datasets (RDD); nó hỗ trợ tính toán xử lý trong bộ nhớ. Điều này có nghĩa, nó lưu trữ trạng thái của bộ nhớ dưới dạng một đối tượng trên các công việc và đối tượng có thể chia sẻ giữa các công việc đó. Việc xử lý dữ liệu trong bộ nhớ nhanh hơn 10 đến 100 lần so với network và disk.

Các loại RDD:
 - RDD of Strings
 - RDD of Pairs
<p align = "center"> <img src = https://mallikarjuna_g.gitbooks.io/spark/content/diagrams/spark-rdds.png>

### Các transformation và action với RDD:
RDD cung cấp các transformation và action hoạt động giống như DataFrame lẫn DataSets. Transformation xử lý các thao tác lazily và Action xử lý thao tác cần xử lý tức thời.
<p align = "center"> <img src = https://i.stack.imgur.com/3QiV8.png>
 
Một số transformation: Nhiều phiên bản transformation của RDD có thể hoạt động trên các Structured API, transformation xử lý lazily, tức là chỉ giúp dựng execution plans, dữ liệu chỉ được truy xuất thực sự khi thực hiện action.
 - distinct: loại bỏ trùng lắp trong RDD.
 - filter: tương đương với việc sử dụng where trong SQL, tìm các record trong RDD xem những phần tử nào thỏa điều kiện. Có thể cung cấp một hàm phức tạp sử dụng để filter các record cần thiết. Như trong Python, ta có thể sử dụng hàm lambda để truyền vào filter.
 - map: thực hiện một công việc nào đó trên toàn bộ RDD. Trong Python sử dụng lambda với từng phần tử để truyền vào map.
 - flatMap: cung cấp một hàm đơn giản hơn hàm map. Yêu cầu output của map phải là một structure có thể lặp và mở rộng được.
 - sortBy: mô tả một hàm để trích xuất dữ liệu từ các object của RDD và thực hiện sort được từ đó.
 - randomSplit: nhận một mảng trọng số và tạo một random seed, tách các RDD thành một mảng các RDD có số lượng chia theo trọng số.
 
Một số action: Action thực thi ngay các transformation đã được thiết lập để thu thập dữ liệu về driver để xử lý hoặc ghi dữ liệu xuống các công cụ lưu trữ.
 - reduce: thực hiện hàm reduce trên RDD để thu về 1 giá trị duy nhất.
 - count: đếm số dòng trong RDD.
 - countApprox: phiên bản đếm xấp xỉ của count, nhưng phải cung cấp timeout vì có thể không nhận được kết quả.
 - countByValue: đếm số giá trị của RDD, chỉ sử dụng nếu map kết quả nhỏ vì tất cả dữ liệu sẽ được load lên memory của driver để tính toán, chỉ nên sử dụng trong tình huống số dòng nhỏ và số lượng item khác nhau cũng nhỏ.
 - countApproxDistinct: đếm xấp xỉ các giá trị khác nhau.
 - countByValueApprox: đếm xấp xỉ các giá trị.
 - first: lấy giá trị đầu tiên của dataset.
 - max và min: lần lượt lấy giá trị lớn nhất và nhỏ nhất của dataset.
 - take và các method tương tự: lấy một lượng giá trị từ trong RDD, take sẽ trước hết scan qua một partition và sử dụng kết quả để dự đoán số lượng partition cần phải lấy thêm để thỏa mãn số lượng lấy.
 - top và takeOrdered: top sẽ hiệu quả hơn takeOrdered vì top lấy các giá trị đầu tiên được sắp xếp ngầm trong RDD.
 - takeSamples: lấy một lượng giá trị ngẫu nhiên trong RDD.

## Spark DataFrame 
### Khái niệm
DataFrame là một kiểu dữ liệu collection phân tán, được tổ chức thành các cột được đặt tên. Về mặt khái niệm, nó tương đương với các bảng quan hệ (relational tables) đi kèm với các kỹ thuật tối ưu tính toán.

<p align = "center"> <img src = https://cdn.helpex.vn/upload/2019/2/19/ar/01-50-19-042-625b5d61-0e62-4f67-81b9-693d9937ea94.jpg>

DataFrame có thể được xây dựng từ nhiều nguồn dữ liệu khác nhau như Hive table, các file dữ liệu có cấu trúc hay bán cấu trúc (csv, json), các hệ cơ sở dữ liệu phổ biến (MySQL, MongoDB, Cassandra), hoặc RDDs hiện hành. API này được thiết kế cho các ứng dụng Big Data và Data Science hiện đại.

### Các tính năng của DataFrame
DataFrame có một số tính năng sau:
 - Khả năng xử lý dữ liệu có kích thước từ Kilobyte đến Petabyte trên một cụm nút đơn đến cụm lớn.
 - Hỗ trợ các định dạng dữ liệu khác nhau (Avro, csv, tìm kiếm đàn hồi và Cassandra) và hệ thống lưu trữ (HDFS, bảng HIVE, mysql, v.v.).
 - Tối ưu hóa hiện đại và tạo mã thông qua trình tối ưu hóa Spark SQL Catalyst (khung chuyển đổi cây).
 - Có thể dễ dàng tích hợp với tất cả các công cụ và khuôn khổ Dữ liệu lớn thông qua Spark-Core.
 - Cung cấp API cho Lập trình Python, Java, Scala và R.

SQLContext:
 - SQLContext là một lớp và được sử dụng để khởi tạo các chức năng của Spark SQL. Đối tượng lớp SparkContext (sc) là bắt buộc để khởi tạo đối tượng lớp SQLContext.

Hoạt động DataFrame:
 - DataFrame cung cấp một ngôn ngữ dành riêng cho miền để thao tác dữ liệu có cấu trúc. Ở đây, chúng tôi bao gồm một số ví dụ cơ bản về xử lý dữ liệu có cấu trúc bằng DataFrames.
 


## Nguồn
https://www.tutorialspoint.com/apache_spark/apache_spark_rdd.htm
https://www.facebook.com/notes/c%E1%BB%99ng-%C4%91%E1%BB%93ng-big-data-vi%E1%BB%87t-nam/apache-spark-fundamentals-ph%E1%BA%A7n-2-spark-core-v%C3%A0-rdd/514714606074061/

