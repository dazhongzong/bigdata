import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta
from pathlib import Path
import time
import re
random.seed(42)
np.random.seed(42)
class FactTablePreprocessor:
    """
    航班事实表预处理器，负责清洗和转换原始数据以满足数据仓库要求
    """
    
    def __init__(self, random_seed=42):
        """初始化预处理器"""
        self.random_seed = random_seed
        random.seed(random_seed)
        np.random.seed(random_seed)
        self.dimension_tables = {}
        self.fact_df = None
    
    def _get_file_path(self, relative_path):
        """获取文件的完整路径，处理不同操作系统的路径分隔符"""
        return os.path.normpath(relative_path)
    
    def load_dimension_table(self, table_name, file_path):
        """
        加载维度表并存储在内存中
        
        Args:
            table_name (str): 维度表名称（用于内部引用）
            file_path (str): Excel文件路径
        """
        try:
            full_path = self._get_file_path(file_path)
            self.dimension_tables[table_name] = pd.read_excel(full_path)
            print(f"成功加载维度表 '{table_name}'，共 {len(self.dimension_tables[table_name])} 条记录")
            return True
        except Exception as e:
            print(f"加载维度表 '{table_name}' 失败: {str(e)}")
            return False
    
    def load_fact_table(self, file_path):
        """
        加载事实表
        
        Args:
            file_path (str): 事实表Excel文件路径
        """
        try:
            full_path = self._get_file_path(file_path)
            self.fact_df = pd.read_excel(full_path)
            print(f"成功加载事实表，共 {len(self.fact_df)} 条记录")
            return True
        except Exception as e:
            print(f"加载事实表失败: {str(e)}")
            return False
    
    def clean_invalid_data(self):
        """
        清理无效数据：删除CabinID或PlaneID为空的记录
        """
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        initial_count = len(self.fact_df)
        # 只保留CabinID和PlaneID都不为空的记录
        self.fact_df = self.fact_df[(~self.fact_df['CabinID'].isna()) & (~self.fact_df['planeID'].isna())]
        final_count = len(self.fact_df)
        print(f"数据清理完成：删除 {initial_count - final_count} 条无效记录，剩余 {final_count} 条有效记录")
        self.fact_df = self.fact_df.reset_index(drop=True)
        return True
    
    def assign_airport_ids(self):
        """
        为出发机场和降落机场分配ID，确保至少300个不同组合
        """
        if 'DW_Dim_Airport' not in self.dimension_tables:
            print("错误：机场维度表未加载")
            return False
        
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        airport_ids = self.dimension_tables['DW_Dim_Airport']['AirportID'].unique().tolist()
        print(f"可用机场ID数量: {len(airport_ids)}")
        
        # 为出发机场和降落机场分配随机ID
        self.fact_df['Departure_airportID'] = np.random.choice(airport_ids, size=len(self.fact_df)).astype(int)
        self.fact_df['Landing_airportID'] = np.random.choice(airport_ids, size=len(self.fact_df)).astype(int)
        
        # 确保出发机场和降落机场不同
        for i in range(len(self.fact_df)):
            while self.fact_df.at[i, 'Departure_airportID'] == self.fact_df.at[i, 'Landing_airportID']:
                self.fact_df.at[i, 'Landing_airportID'] = np.random.choice(airport_ids).astype(int)
        departures = sorted(self.fact_df['Departure_airportID'].unique().tolist())
        print(set(departures) - set(airport_ids))
        unique_departures = self.fact_df['Departure_airportID'].nunique()
        unique_landings = self.fact_df['Landing_airportID'].nunique()
        print(f"已分配机场ID - 出发机场唯一值: {unique_departures}, 降落机场唯一值: {unique_landings}")
        return True
    
    def assign_transit_airports(self, min_transit_flights=200, transit_ratio=0.2):
        """
        为航班分配经停机场ID，确保至少有指定数量的经停航班
        
        Args:
            min_transit_flights (int): 最小经停航班数量
            transit_ratio (float): 经停航班比例（如果按比例计算超过最小值）
        """
        if 'DW_Dim_Airport' not in self.dimension_tables:
            print("错误：机场维度表未加载")
            return False
        
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        airport_ids = self.dimension_tables['DW_Dim_Airport']['AirportID'].unique().tolist()
        
        # 计算经停航班数量
        suggested_count = int((len(self.fact_df)) * transit_ratio)
        transit_count = max(min_transit_flights, suggested_count)
        print(f"计划分配经停机场 - 最小要求: {min_transit_flights}, 按比例计算: {suggested_count}, 实际分配: {transit_count}")
        
        # 创建经停掩码
        transit_mask = np.zeros(len(self.fact_df), dtype=bool)
        transit_indices = np.random.choice(len(self.fact_df), size=transit_count, replace=False)
        transit_mask[transit_indices] = True
        
        # 初始化经停机场ID列
        self.fact_df['Transit_airportID'] = np.nan
        self.fact_df.loc[transit_mask, 'Transit_airportID'] = np.random.choice(airport_ids, size=transit_count)
        
      # 确保经停机场与出发/降落机场不同
        transit_count_before_fix = sum(transit_mask)
        for i in self.fact_df[transit_mask].index:
            temp_ids = airport_ids.copy()
            if self.fact_df.at[i, 'Transit_airportID'] in temp_ids:  # 确保经停机场ID在可用列表中
                temp_ids.remove(self.fact_df.at[i, 'Transit_airportID'])
            if self.fact_df.at[i, 'Transit_airportID'] in temp_ids:  # 确保经停机场ID在可用列表中
                temp_ids.remove(self.fact_df.at[i, 'Transit_airportID'])
            while (self.fact_df.at[i, 'Transit_airportID'] == self.fact_df.at[i, 'Departure_airportID'] or
                   self.fact_df.at[i, 'Transit_airportID'] == self.fact_df.at[i, 'Landing_airportID']):
                self.fact_df.at[i, 'Transit_airportID'] = int(np.random.choice(temp_ids))
                
        actual_transit_count = self.fact_df['Transit_airportID'].notna().sum()
        print(f"经停机场分配完成 - 要求数量: {transit_count}, 实际数量: {actual_transit_count}")
        self.fact_df['Transit_airportID'] = self.fact_df['Transit_airportID'].fillna(-1)
        self.fact_df['Transit_airportID'] = self.fact_df['Transit_airportID'].astype(int)

        return True

    def assign_ticket_ids(self, min_unique_flights=600):
        """
        为事实表分配TicketID，确保通过Ticket维表关联至少600个不同航班
        
        策略:
        1. 检查Ticket维表是否满足最小唯一航班数量
        2. 如果不满足，修改维表增加航班数量
        3. 为事实表分配TicketID：
        - 首先依次分配600个不同航班的TicketID
        - 剩余记录从这600个航班中随机分配TicketID
        
        Args:
            min_unique_flights (int): 最小唯一航班数量(通过FlightNo区分)
        """
        
        if 'DW_Dim_Ticket' not in self.dimension_tables:
            print("错误：机票维度表未加载")
            return False
        
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        # 重置事实表索引
        ticket_dim = self.dimension_tables['DW_Dim_Ticket'].copy()
        
        # 检查必要列
        required_columns = ['TicketID', 'FlightNo']
        missing_columns = [col for col in required_columns if col not in ticket_dim.columns]
        if missing_columns:
            print(f"错误：机票维度表缺少必要列: {', '.join(missing_columns)}")
            return False
        
        print(f"开始TicketID分配流程 - 要求唯一航班数: {min_unique_flights}")
        print(f"当前Ticket维表记录数: {len(ticket_dim)}, 唯一航班数: {ticket_dim['FlightNo'].nunique()}")
        
        # 1. 检查并扩充Ticket维表（如果需要）
        current_unique_flights = ticket_dim['FlightNo'].nunique()
        flights_to_add = max(0, min_unique_flights - current_unique_flights)
        
        if flights_to_add > 0:
            print(f"⚠️  Ticket维表航班数量不足({current_unique_flights})，需要增加{flights_to_add}个航班")
            ticket_dim = self._expand_ticket_dimension_flights(ticket_dim, flights_to_add)
            print(f"扩充后唯一航班数: {ticket_dim['FlightNo'].nunique()}")
        
        # 2. 为事实表分配TicketID
        success = self._assign_ticket_ids_sequential_then_random(ticket_dim, min_unique_flights)
        
        if success:
            # 3. 验证分配结果
            assigned_tickets = self.fact_df['TicketID'].unique()
            assigned_flights = ticket_dim[ticket_dim['TicketID'].isin(assigned_tickets)]['FlightNo'].nunique()
            print(f"✅ 分配成功 - 覆盖唯一航班: {assigned_flights} (要求: {min_unique_flights})")
            
            # 更新维度表
            self.dimension_tables['DW_Dim_Ticket'] = ticket_dim
            # 保存更新后的Ticket维度表
            self.dimension_tables['DW_Dim_Ticket'].to_excel('output/DW_Dim_Ticket_updated.xlsx', index=False)
            print("已保存更新后的Ticket维度表")
            return True
        else:
            print("❌ TicketID分配失败")
            return False

    def _expand_ticket_dimension_flights(self, ticket_dim, flights_to_add):
        """
        扩充Ticket维度表，增加指定数量的唯一航班号
        
        Args:
            ticket_dim (DataFrame): 原始Ticket维度表
            flights_to_add (int): 需要增加的唯一航班数量
        
        Returns:
            DataFrame: 扩充后的Ticket维度表
        """
        import pandas as pd
        import numpy as np
        import re
        import random
        
        # 获取现有航班
        existing_flights = set(ticket_dim['FlightNo'].unique())
        original_count = len(existing_flights)
        print(f"扩展前唯一航班数: {original_count}")
        
        # 创建新记录
        new_records = []
        added_flights = set()
        
        # 获取现有记录作为模板
        templates = ticket_dim.sample(n=min(flights_to_add, len(ticket_dim)), random_state=42).to_dict('records')
        
        for i in range(flights_to_add):
            if i < len(templates):
                template = templates[i].copy()
            else:
                # 如果模板不够，循环使用
                template = templates[i % len(templates)].copy()
            
            # 生成新航班号
            original_flight = template['FlightNo']
            prefix = self._extract_airline_prefix(original_flight)
            new_flight_no = self._generate_unique_flight_no(existing_flights | added_flights, prefix, original_flight)
            
            if new_flight_no:
                # 创建新记录
                new_record = template.copy()
                # 生成新的TicketID（避免重复）
                max_ticket_id = ticket_dim['TicketID'].max() if 'TicketID' in ticket_dim.columns else 1000000
                new_record['TicketID'] = max_ticket_id + 1000 + i  # 确保不重复
                new_record['FlightNo'] = new_flight_no
                
                new_records.append(new_record)
                added_flights.add(new_flight_no)
        
        print(f"计划添加 {len(new_records)} 条新记录，增加 {len(added_flights)} 个新航班")
        
        # 转换为DataFrame并合并
        if new_records:
            new_df = pd.DataFrame(new_records)
            ticket_dim = pd.concat([ticket_dim, new_df], ignore_index=True)
            print(f"扩展后唯一航班数: {ticket_dim['FlightNo'].nunique()}")
        
        return ticket_dim
    def _assign_ticket_ids_sequential_then_random(self, ticket_dim, min_unique_flights):
        """
        为事实表分配TicketID，先用完所有不同航班，再随机分配
        
        策略:
        1. 按FlightNo分组，获取每个航班的TicketID列表
        2. 依次从每个航班组中取TicketID，直到用完所有min_unique_flights个航班
        3. 剩余记录从已使用的航班中随机选择TicketID
        
        Args:
            ticket_dim (DataFrame): Ticket维度表
            min_unique_flights (int): 要使用的最小唯一航班数量
        """
        import numpy as np
        import pandas as pd
        import random
        
        # 1. 按FlightNo分组，获取每个航班的TicketID
        flight_groups = ticket_dim.groupby('FlightNo')['TicketID'].apply(list).reset_index()
        print(f"可用航班组数量: {len(flight_groups)}")
        
        # 2. 选择要使用的航班
        flights_to_use = flight_groups.head(min(len(flight_groups), min_unique_flights))
        print(f"将使用 {len(flights_to_use)} 个唯一航班")
        
        if len(flights_to_use) == 0:
            print("错误：没有可用的航班")
            return False
        
        # 3. 为事实表创建TicketID列
        self.fact_df['TicketID'] = None
        
        total_records = len(self.fact_df)
        unique_flights_count = len(flights_to_use)
        
        # 4. 策略1: 事实表记录数 <= 唯一航班数
        if total_records <= unique_flights_count:
            print(f"事实表记录数({total_records}) <= 唯一航班数({unique_flights_count})，分配唯一航班")
            
            for i in range(total_records):
                flight_row = flights_to_use.iloc[i % unique_flights_count]
                # 从该航班的TicketID列表中选择一个
                ticket_options = flight_row['TicketID']
                self.fact_df.at[i, 'TicketID'] = random.choice(ticket_options) if ticket_options else None
        
        # 5. 策略2: 事实表记录数 > 唯一航班数
        else:
            print(f"事实表记录数({total_records}) > 唯一航班数({unique_flights_count})，优先分配所有唯一航班")
            
            # 5.1 首先为前unique_flights_count条记录分配不同航班
            for i in range(unique_flights_count):
                flight_row = flights_to_use.iloc[i]
                ticket_options = flight_row['TicketID']
                self.fact_df.at[i, 'TicketID'] = random.choice(ticket_options) if ticket_options else None
            
            print(f"✅ 已分配 {unique_flights_count} 个唯一航班")
            
            # 5.2 收集所有已使用的TicketID，用于剩余记录的随机分配
            used_ticket_ids = []
            for _, row in flights_to_use.iterrows():
                used_ticket_ids.extend(row['TicketID'])
            
            if not used_ticket_ids:
                print("错误：没有可用的TicketID")
                return False
            
            # 5.3 为剩余记录随机分配TicketID
            remaining_count = total_records - unique_flights_count
            print(f"为剩余 {remaining_count} 条记录随机分配TicketID")
            
            # 使用numpy高效分配
            random_tickets = np.random.choice(used_ticket_ids, size=remaining_count)
            
            for i, ticket_id in enumerate(random_tickets, start=unique_flights_count):
                self.fact_df.at[i, 'TicketID'] = ticket_id
        
        # 6. 验证分配结果
        null_count = self.fact_df['TicketID'].isnull().sum()
        if null_count > 0:
            print(f"警告：{null_count} 条记录未分配TicketID")
            # 为未分配的记录填充随机TicketID
            all_ticket_ids = ticket_dim['TicketID'].unique()
            if len(all_ticket_ids) > 0:
                fill_tickets = np.random.choice(all_ticket_ids, size=int(null_count))
                null_indices = self.fact_df[self.fact_df['TicketID'].isnull()].index
                for idx, ticket_id in zip(null_indices, fill_tickets):
                    self.fact_df.at[idx, 'TicketID'] = ticket_id
        
        # 7. 转换数据类型
        try:
            self.fact_df['TicketID'] = self.fact_df['TicketID'].astype('Int64')
        except Exception as e:
            print(f"转换TicketID数据类型时出错: {e}")
        
        return True
    def _extract_airline_prefix(self, flight_number):
        """
        从航班号中提取航空公司前缀
        
        Args:
            flight_number (str/any): 航班号
        
        Returns:
            str: 航空公司前缀（如'CA', 'MU', 'CZ'等）
        """
        import re
        
        if pd.isna(flight_number) or not str(flight_number).strip():
            return 'CA'  # 默认中国国航
        
        flight_str = str(flight_number).strip().upper()
        
        # 提取前1-2个字母作为前缀
        prefix_match = re.match(r'^([A-Z]{1,2})', flight_str)
        if prefix_match:
            return prefix_match.group(1)
        
        # 如果没有字母前缀，使用默认
        return 'CA'

    def _generate_unique_flight_no(self, existing_flights, prefix, original_flight=None):
        """
        生成一个唯一的航班号
        
        Args:
            existing_flights (set): 现有航班号集合
            prefix (str): 航空公司前缀
            original_flight (str): 原始航班号（可选，用于参考）
        
        Returns:
            str: 唯一的航班号
        """
        import random
        import time
        import re
        
        max_attempts = 1000
        
        for attempt in range(max_attempts):
            if attempt == 0 and original_flight:
                # 尝试在原始航班号基础上修改
                orig_num = re.sub(r'^[A-Z]+', '', str(original_flight))
                if orig_num.isdigit() and 100 <= int(orig_num) <= 9999:
                    new_num = (int(orig_num) + 1000) % 10000 + 100  # 确保4位数
                    candidate = f"{prefix}{new_num:04d}"
                else:
                    candidate = f"{prefix}{random.randint(1000, 9999)}"
            else:
                # 生成随机航班号
                candidate = f"{prefix}{random.randint(1000, 9999)}"
            
            if candidate not in existing_flights:
                return candidate
        
        # 所有尝试失败，使用时间戳作为最后手段
        timestamp = int(time.time() * 1000) % 10000
        fallback = f"{prefix}{timestamp:04d}"
        
        # 确保回退方案也是唯一的
        counter = 0
        while fallback in existing_flights and counter < 100:
            fallback = f"{prefix}{(timestamp + counter) % 10000:04d}"
            counter += 1
        
        if fallback in existing_flights:
            print(f"警告：无法生成唯一个性化航班号，使用随机回退方案")
            return f"{prefix}{random.randint(90000, 99999)}"
        
        return fallback
    def assign_passenger_ids(self, min_unique_passengers=600, max_extra_passengers=400):
        """
        为航班分配乘客ID，确保至少有指定数量的唯一乘客
        
        Args:
            min_unique_passengers (int): 最小唯一乘客数量
            max_extra_passengers (int): 在最小值基础上最多增加的额外乘客数量
        """
        if 'DW_Dim_Passenger' not in self.dimension_tables:
            print("错误：乘客维度表未加载")
            return False
        
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        passenger_ids = self.dimension_tables['DW_Dim_Passenger']['PassengerID'].unique().tolist()
        print(f"从维度表获取到 {len(passenger_ids)} 个唯一乘客ID")
        
        # 随机选择乘客ID
        random.shuffle(passenger_ids)
        max_possible = min(len(passenger_ids), min_unique_passengers + max_extra_passengers)
        actual_count = min(random.randint(min_unique_passengers, max_possible), len(passenger_ids))
        selected_passenger_ids = passenger_ids[:actual_count]
        
        print(f"随机选择 {actual_count} 个乘客ID (范围: {min_unique_passengers}-{max_possible})")
        
        # 为事实表分配乘客ID
        self.fact_df['MEMBER_NO'] = np.random.choice(selected_passenger_ids, size=len(self.fact_df))
        
        actual_unique = self.fact_df['MEMBER_NO'].nunique()
        print(f"乘客ID分配完成 - 要求至少 {min_unique_passengers} 个唯一值，实际有 {actual_unique} 个唯一值")
        return True

    def assign_plane_and_cabin_ids(self):
        """
        为航班分配飞机ID和舱位ID
        """
        success = True
        
        # 分配飞机ID
        if 'DW_Dim_Plane' in self.dimension_tables and 'DW_Dim_Seat' in self.dimension_tables:
            plane_ids = self.dimension_tables['DW_Dim_Plane']['PlaneID'].unique().tolist()
            self.fact_df['planeID'] = np.random.choice(plane_ids, size=len(self.fact_df))
            print(f"已分配飞机ID，共 {len(plane_ids)} 个唯一值")
            cabin_ids = self.dimension_tables['DW_Dim_Seat']['CabinID'].unique().tolist()
            """
            根据planeID分配舱位ID的逻辑可以更复杂一些，例如根据飞机类型分配不同的舱位。
            planeID 有对应的seatnum，使用seatnum限制cabinID的分配范围。
            """
            planeID_to_seatnum = self.dimension_tables['DW_Dim_Plane'].set_index('PlaneID')['SeatNum'].to_dict()

            for idx,row in self.fact_df.iterrows():
                # 根据planeID获取对应的seatNum
                plane_id = row['planeID']
                seat_num = planeID_to_seatnum.get(plane_id, None)
                if seat_num is not None:
                    # 假设cabinID与seatNum有某种关系，这里简单示例为随机选择
                    possible_cabins = [cid for cid in cabin_ids if cid <= seat_num]
                    if possible_cabins:
                        self.fact_df.at[idx, 'CabinID'] = np.random.choice(possible_cabins)
                    else:
                        self.fact_df.at[idx, 'CabinID'] = np.nan  # 如果没有合适的舱位，设为NaN
                else:
                    self.fact_df.at[idx, 'CabinID'] = np.nan  # 如果没有对应的seatNum，设为NaN
            print(f"已分配舱位ID，共 {len(cabin_ids)} 个唯一值")
        else:
            print("警告：飞机、座椅维度表未加载,无法分配飞机ID")
            success = False
        
        return success
    
    def generate_flight_dates(self, min_days_span=180):
        """
        生成航班日期，确保日期跨度至少为指定天数，并且与Ticket维表中的出发和到达时间相匹配。
        
        Args:
            min_days_span (int): 最小日期跨度（天）
        """
        if self.fact_df is None or 'DW_Dim_Ticket' not in self.dimension_tables:
            print("错误：事实表未加载或Ticket维表未加载")
            return False
        
        # 获取所有唯一的TicketID
        ticket_ids = self.fact_df['TicketID'].unique()
        ticket_dim = self.dimension_tables['DW_Dim_Ticket']
        
        # 对于每一个TicketID，从ticket维表中获取出发和到达时间，并分配到事实表中
        for tid in ticket_ids:
            ticket_info = ticket_dim[ticket_dim['TicketID'] == tid]
            if not ticket_info.empty:
                departure_time = (ticket_info['departureTime'].iloc[0])[:10]
                arrival_time = (ticket_info['arrivalTime'].iloc[0])[:10]
                
                # 假设DateRangeDim有一个名为'date'的列表示具体日期
                date_range_dim = self.dimension_tables.get('DW_Dim_DateRange')
                if date_range_dim is not None:
                    dep_date_id = date_range_dim[date_range_dim['Date'] == departure_time]['DateRangeID'].values
                    arr_date_id = date_range_dim[date_range_dim['Date'] == arrival_time]['DateRangeID'].values
                    
                    if len(dep_date_id) > 0 and len(arr_date_id) > 0:
                        # 更新事实表中对应TicketID的出发和到达日期ID
                        self.fact_df.loc[self.fact_df['TicketID'] == tid, 'departureDateID'] = (dep_date_id[0])
                        self.fact_df.loc[self.fact_df['TicketID'] == tid, 'arrivalDateID'] = (arr_date_id[0])
        
        # 验证日期跨度是否满足要求
        min_date = self.fact_df['departureDateID'].min()
        max_date = self.fact_df['departureDateID'].max()
        span_in_days = (self._get_date_from_date_range_id(max_date) - self._get_date_from_date_range_id(min_date)).days
        
        if span_in_days < min_days_span:
            print(f"警告：实际日期跨度({span_in_days}天)小于最小要求({min_days_span}天)")
        else:
            print(f"航班日期生成完成，日期跨度: {span_in_days} 天")
        return True
    
    def _get_date_from_date_range_id(self, date_range_id):
        """
        根据DateRangeID从日期维度表中获取对应的具体日期
        
        Args:
            date_range_id (int): DateRangeID
            
        Returns:
            datetime: 对应的具体日期
        """
        date_range_dim = self.dimension_tables.get('DW_Dim_DateRange')
        if date_range_dim is not None:
            specific_date = date_range_dim[date_range_dim['DateRangeID'] == date_range_id]['Date']
            if not specific_date.empty:
                return pd.to_datetime(specific_date.values[0])
        return None


    def assign_price_and_mileage(self):
        """
        为航班分配价格范围、里程和评分
        基于机场坐标计算真实里程，直飞航班计算起点到终点距离
        中转航班计算起点到中转点、中转点到终点的距离之和
        """
        if self.fact_df is None:
            print("错误：事实表未加载")
            return False
        
        # 检查机场维度表是否存在
        if 'DW_Dim_Airport' not in self.dimension_tables:
            print("错误：机场维度表未加载，无法计算里程")
            return False
        
        airport_dim = self.dimension_tables['DW_Dim_Airport']
        
        # 创建机场代码到坐标的映射字典
        airport_coords = {}
        for _, row in airport_dim.iterrows():
            airport_coords[row['AirportID']] = (row['airport_x'], row['airport_y'])
        
        print(f"已加载 {len(airport_coords)} 个机场的坐标数据")
        
        # 计算每个航班的里程
        mileages = []
        
        
        for _, row in self.fact_df.iterrows():
            origin_id = row['Departure_airportID']
            dest_id = row['Landing_airportID']
            transfer_id = row.get('Transit_airportID', None)  # 使用get避免KeyError
            
            # 检查必要的机场是否存在坐标
            if origin_id not in airport_coords or dest_id not in airport_coords:
                print(f"警告：缺少机场坐标数据，OriginID={origin_id}, DestID={dest_id}")
                mileages.append(np.random.randint(500, 5000))  # 使用随机值作为回退
                continue
            
            # 计算里程 - 欧几里得距离
            if pd.isna(transfer_id) or transfer_id not in airport_coords:
                # 直飞航班
                origin_x, origin_y = airport_coords[origin_id]
                dest_x, dest_y = airport_coords[dest_id]
                distance = np.sqrt((dest_x - origin_x)**2 + (dest_y - origin_y)**2)
                mileages.append(distance)
            else:
                # 中转航班
                origin_x, origin_y = airport_coords[origin_id]
                transfer_x, transfer_y = airport_coords[transfer_id]
                dest_x, dest_y = airport_coords[dest_id]
                
                # 起点到中转点
                leg1 = np.sqrt((transfer_x - origin_x)**2 + (transfer_y - origin_y)**2)
                # 中转点到终点
                leg2 = np.sqrt((dest_x - transfer_x)**2 + (dest_y - transfer_y)**2)
                
                total_distance = leg1 + leg2
                mileages.append(total_distance)
        
        # 将计算的里程添加到事实表
        self.fact_df['mileage'] = mileages
        
        # # 分配价格范围ID
        # if 'DW_Dim_PriceRange' in self.dimension_tables:
        #     price_range_ids = self.dimension_tables['DW_Dim_PriceRange']['PriceRangeID'].unique().tolist()
        #     self.fact_df['PriceRangeID'] = np.random.choice(price_range_ids, size=len(self.fact_df))
        #     print(f"已分配价格范围ID，共 {len(price_range_ids)} 个唯一值")
        # else:
        #     print("警告：价格范围维度表未加载，无法分配价格范围ID")
        
        # # 生成价格 - 基于里程计算，假设每单位里程0.8-1.2元
        # base_price_per_mile = np.random.uniform(0.8, 1.2, size=len(self.fact_df))
        # self.fact_df['Price'] = np.round(self.fact_df['mileage'] * base_price_per_mile).astype(int)
        
        # # 添加价格波动（基于航班需求、时间等因素）
        # demand_factor = np.random.uniform(0.9, 1.5, size=len(self.fact_df))
        # self.fact_df['Price'] = (self.fact_df['Price'] * demand_factor).astype(int)
        
        # # 确保价格在合理范围内
        # self.fact_df['Price'] = self.fact_df['Price'].clip(lower=300, upper=15000)
        
        # # 生成评分 - 里程较长的航班可能评分略高
        # mileage_factor = 0.1 * (self.fact_df['mileage'] / self.fact_df['mileage'].max())
        # base_rate = np.random.uniform(0.7, 1.0, size=len(self.fact_df))
        # self.fact_df['Rate'] = np.round(base_rate + mileage_factor, 2).clip(0.0, 5.0)
        
        # print(f"已计算 {len(self.fact_df)} 个航班的里程、价格和评分数据")
        print(f"里程范围: {min(mileages):.1f} - {max(mileages):.1f}")
        # print(f"价格范围: {self.fact_df['Price'].min()} - {self.fact_df['Price'].max()}")
        
        return True
    
    def validate_data(self):
        """
        验证预处理后的数据是否满足所有要求
        
        Returns:
            dict: 验证结果
        """
        if self.fact_df is None:
            print("错误：事实表未加载")
            return {}
        
        results = {
            "total_rows": len(self.fact_df),
            "unique_departure_airports": self.fact_df['Departure_airportID'].nunique(),
            "unique_landing_airports": self.fact_df['Landing_airportID'].nunique(),
            "transit_flights_count": self.fact_df['Transit_airportID'].notna().sum(),
            "unique_tickets": self.fact_df['TicketID'].nunique(),
            "unique_planes": self.fact_df['planeID'].nunique(),
            "unique_cabins": self.fact_df['CabinID'].nunique(),
            "date_span_days": (self.fact_df['departureDateID'].max() - self.fact_df['departureDateID'].min()),
            "missing_cabin_id": self.fact_df['CabinID'].isna().sum(),
            "missing_plane_id": self.fact_df['planeID'].isna().sum(),
            "passenger_count": self.fact_df['MEMBER_NO'].sum()
        }
        
        print("\n===== 数据验证结果 =====")
        print(f"总记录数: {results['total_rows']}")
        print(f"唯一起飞机场数: {results['unique_departure_airports']}")
        print(f"唯一降落机场数: {results['unique_landing_airports']}")
        print(f"经停航班数: {results['transit_flights_count']}")
        print(f"唯一TicketID数: {results['unique_tickets']} (要求: 至少600)")
        print(f"唯一飞机数: {results['unique_planes']}")
        print(f"唯一舱位数: {results['unique_cabins']}")
        print(f"乘客数: {results['passenger_count']}")
        print(f"日期跨度(天): {results['date_span_days']}")
        print(f"缺失CabinID的记录数: {results['missing_cabin_id']}")
        print(f"缺失PlaneID的记录数: {results['missing_plane_id']}")
        print("=======================\n")
        
        return results
    
    def save_processed_data(self, output_path):
        """
        保存预处理后的数据
        
        Args:
            output_path (str): 输出文件路径
        """
        if self.fact_df is None:
            print("错误：事实表未加载或未处理")
            return False
        
        try:
            # 确保输出目录存在
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            full_path = self._get_file_path(output_path)
            self.fact_df.to_excel(full_path, index=False)
            print(f"成功保存预处理数据到: {full_path}")
            return True
        except Exception as e:
            print(f"保存数据失败: {str(e)}")
            return False

# 主执行函数
def main():
    """主函数：执行完整的数据预处理流程"""
    # 创建预处理器
    preprocessor = FactTablePreprocessor(random_seed=42)
    
    # 1. 加载所有维度表
    dimension_tables = {
        'DW_Dim_Airport': 'data\dim\DW_Dim_Airport.xlsx',
        'DW_Dim_Passenger': 'data\dim\DW_Dim_Passenger.xlsx',
        'DW_Dim_Plane': 'data\dim\DW_Dim_Plane.xlsx',  
        'DW_Dim_Seat': 'data\dim\DW_Dim_Seat.xlsx',    
        'DW_Dim_PriceRange': 'data\dim\DW_Dim_PriceRange.xlsx',  
        'DW_Dim_Ticket': 'data\dim\DW_Dim_ticket.xlsx',
        'DW_Dim_DateRange' : 'data\dim\DW_Dim_DateRange.xlsx'
    }
    
    for table_name, file_path in dimension_tables.items():
        preprocessor.load_dimension_table(table_name, file_path)
    
    # 2. 加载事实表
    preprocessor.load_fact_table('data/FactTG12.xlsx')
    
    # 3. 执行数据预处理步骤
    processing_steps = [
        ("清理无效数据", preprocessor.clean_invalid_data),
        ("分配机场ID", preprocessor.assign_airport_ids),
        ("分配经停机场", lambda: preprocessor.assign_transit_airports(min_transit_flights=200)),
        ("分配TicketID", lambda: preprocessor.assign_ticket_ids(min_unique_flights=600)),  # 新增TicketID处理
        ("分配乘客ID", lambda: preprocessor.assign_passenger_ids(min_unique_passengers=600)),
        ("分配飞机和舱位ID", preprocessor.assign_plane_and_cabin_ids),
        ("生成航班日期", lambda: preprocessor.generate_flight_dates(min_days_span=180)),
        ("分配价格和里程", preprocessor.assign_price_and_mileage),
        ("验证数据", preprocessor.validate_data)
    ]
    

    for step_name, step_function in processing_steps:
        print(f"\n===== {step_name} =====")
        try:
            result = step_function()
            if result is False:
                print(f"步骤 '{step_name}' 失败")
        except Exception as e:
            print(f"步骤 '{step_name}' 执行出错: {str(e)}")
    
    # 4. 保存结果
    print("\n===== 保存处理结果 =====")
    preprocessor.save_processed_data('output/output.xlsx')
    
    print("\n数据预处理流程完成")

# 程序入口
if __name__ == "__main__":
    main()