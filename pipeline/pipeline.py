import time

import pyarrow.parquet as pq
from sqlalchemy import create_engine

# 1. 连接到你的PostgreSQL容器
# 格式: postgresql://用户名:密码@主机:端口/数据库名
# 注意：主机名用容器名或IP。如果Docker在Win11上使用默认网络，主机可能是localhost或127.0.0.1
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# 2. 指定你的Parquet文件路径
parquet_file_path = './data/green_tripdata_2025-11.parquet'

# 3. 使用PyArrow打开文件并获取行组信息
print("开始处理Parquet文件...")
parquet_file = pq.ParquetFile(parquet_file_path)
total_row_groups = parquet_file.num_row_groups
print(f"文件共有 {total_row_groups} 个行组。")

# 4. 逐行组读取、转换、写入
for i in range(total_row_groups):
    start_time = time.time()
    
    # 读取一个行组（一个块）
    table = parquet_file.read_row_group(i)
    # 转换为Pandas DataFrame
    df_chunk = table.to_pandas()
    
    # 关键：将当前数据块写入数据库表
    # if_exists='append' 表示追加到现有表
    df_chunk.to_sql(
        name='green_trip', # 目标表名
        con=engine,
        if_exists='append',        # 追加模式
        index=False,               # 不写入DataFrame的索引
        method='multi',            # 批量插入，极大提升速度
        chunksize=5000            # 即使在块内，也分小批提交
    )
    
    # 计算并打印进度
    chunk_time = time.time() - start_time
    print(f"已处理行组 {i+1}/{total_row_groups} [约{len(df_chunk)}行], 耗时: {chunk_time:.2f}秒")
    
    # 可选：手动释放内存（通常GC会自动处理，但显式释放更稳妥）
    del table, df_chunk

print("所有数据已成功写入PostgreSQL!")
