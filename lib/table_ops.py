import os
import pandas as pd
import csv
import glob
from typing import Optional, Union, Dict # python3.5+

def _read_file(
    file_path: str,
    encoding: Optional[str] = None,
    sheet_name: Union[str, int] = 0
) -> pd.DataFrame:  
    """
    读取单个表格文件（CSV/Excel）并返回 DataFrame。

    参数:
        file_path: 文件路径
        encoding: 读取 CSV 时的编码（None 则尝试 utf-8 和 gbk）
        sheet_name: Excel 工作表名称或索引（默认第 0 个表）

    返回:
        pd.DataFrame: 读取的数据

    异常:
        ValueError: 不支持的文件格式或编码尝试失败
        RuntimeError: 读取文件时发生其他错误
    """
    try:
        if file_path.endswith('.csv'):
            encodings = [encoding] if encoding else ['utf-8', 'gbk']
            for enc in encodings:
                try:
                    return pd.read_csv(file_path, encoding=enc)
                except UnicodeDecodeError:
                    continue
            raise ValueError(f"Failed to parse the CSV file {file_path}, attempted encodings: {encodings}")         
        elif file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, sheet_name=sheet_name)            
        else:
            raise ValueError(f"Unsupported file format: {file_path}, supported formats are .csv, .xlsx")
            
    except Exception as e:
        raise RuntimeError(f"Failed to read file {file_path}: {str(e)}")
    
def table_merge(
        source_dir:str,
        output_path:str = "./merged.csv",
        output_format:str = "csv"
) -> None:
    """
    合并指定目录下的表格文件（csv/xlsx）并输出为指定格式。

    参数:
        source_dir: 源文件所在目录路径
        output_path: 合并后文件的保存路径
        output_format: 输出文件格式，支持 'csv'/'xlsx'，默认 'csv'

    异常:
        ValueError: 当输出路径后缀与指定格式不匹配时抛出
        Exception: 文件读取或写入失败时捕获并打印异常
    """
    supported_formats = ('.csv', '.xlsx')
    all_files = [file for file in glob.glob(os.path.join(source_dir, "*")) if file.endswith(supported_formats)]
    
    if not all_files:
        print("No supported files found in the source directory")
        return
    
    df_list = []
    for file in all_files:
        try:
            df = _read_file(file)
            df_list.append(df)
        except Exception as e:
            print(f"Skipping file {file} due to error: {str(e)}")  
            continue
    if not df_list:
        print("No valid data to merge after reading files")
        return
    combined_df = pd.concat(df_list, ignore_index=True)

    try:
        if output_format.lower() == "csv":
            combined_df.to_csv(output_path, index=False)
        elif output_format.lower() == "xlsx":
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                combined_df.to_excel(writer, index=False)
        print(f"File has been merged: {os.path.abspath(output_path)}")
    except Exception as e:
        print(f"Failed to merge the file: {e}")  

def table_match(
    source_file: str,
    target_file: str,
    source_column: str,
    target_column: str,
    output_file: str,
    ignore_case: bool = False,
    source_sheet: Union[str, int] = 0,
    target_sheet: Union[str, int] = 0,
    drop_duplicates: bool = True
) -> Dict[str, int]:
    """
    将 source_file 中指定列的值，与 target_file 中指定列进行匹配，
    找出 target_file 中“命中 source 列值”的所有行并导出为新文件。

    参数说明：
    ----------
    source_file : str
        源文件路径
    target_file : str
        目标文件路径
    source_column : str
        源文件中用于匹配的列名
    target_column : str
        目标文件中用于匹配的列名
    output_file : str
        匹配结果导出的文件路径
    ignore_case : bool, optional
        是否忽略列名大小写（默认 False）
    source_sheet : Union[str, int], optional
        源文件 sheet 名或索引（默认第 0 个 sheet）
    target_sheet : Union[str, int], optional
        目标文件 sheet 名或索引（默认第 0 个 sheet）
    drop_duplicates : bool, optional
        是否对 source 列去重（默认 True）

    返回值：
    -------
    Dict[str, int]
        返回匹配统计信息：
        - source_total: 源列用于匹配的值数量
        - matched_count: 成功匹配的目标行数
        - target_total: 目标表总行数
        - unmatched_count: 未在目标表中找到的源值数量
    """


    source_df = _read_file(source_file, sheet_name=source_sheet)
    target_df = _read_file(target_file, sheet_name=target_sheet)
    
    if ignore_case:
        source_col_map = {col.lower(): col for col in source_df.columns}
        target_col_map = {col.lower(): col for col in target_df.columns}
        
        source_col = source_col_map.get(source_column.lower())
        target_col = target_col_map.get(target_column.lower())
        
        if not source_col:
            raise ValueError(f"The column {source_column} was not found in the source file (case-insensitive)")
        if not target_col:
            raise ValueError(f"The column {target_column} was not found in the target file (case-insensitive)")
    else:
        source_col = source_column
        target_col = target_column
        
        if source_col not in source_df.columns:
            raise ValueError(f"Column not found in the source file {source_column}")
        if target_col not in target_df.columns:
            raise ValueError(f"Column not found in the target file {target_column}")
    
    source_values = source_df[source_col].dropna()
    if drop_duplicates:
        source_values = source_values.drop_duplicates()
    source_list = source_values.tolist()
    
    matched_df = target_df[target_df[target_col].isin(source_list)]

    target_values = set(target_df[target_col].dropna().unique())
    unmatched_values = [value for value in source_list if value not in target_values]
    
    excel_generate(matched_df, output_file)
    
    return {
        "source_total": len(source_values),
        "matched_count": len(matched_df),
        "target_total": len(target_df),
        "unmatched_count": len(unmatched_values)
    }

def excel_generate(
    data: pd.DataFrame,
    file_path: str,
    sheet_name: str = "Sheet1"
) -> None:
    """
    将 DataFrame 数据写入 Excel 文件。

    参数:
        data: DataFrame 数据
        file_path: Excel 文件保存路径
        sheet_name: Excel 文件的 sheet 名称，默认为 "Sheet1"
    """

    dir_path = os.path.dirname(file_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Unsupported data type: {type(data)}")
    try:
        if file_path.endswith(".csv"):
            data.to_csv(file_path, index=False, encoding="utf-8-sig")
        else:
            with pd.ExcelWriter(file_path, engine = "openpyxl") as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"File has been generated: {os.path.abspath(file_path)}")
    except Exception as e:
        print(f"Failed to generate the file: {e}")

def csv_generate(
    data: list, 
    file_path: str
) -> None:
    """
    将列表数据写入 CSV 文件。

    参数:
        data: 列表数据
        file_path: CSV 文件保存路径
    """
    
    if not data:
        print("No data can be written")
        return None
    
    dir_path = os.path.dirname(file_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    if not isinstance(data[0], dict):
        raise TypeError(f"Unsupported data type: {type(data)}")
    
    try:
        with open(file_path, 'w', newline='', encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        abs_path = os.path.abspath(file_path)
        print(f"File has been generated: {abs_path}")
        return abs_path

    except Exception as e:
        print(f"Failed to generate the file: {e}")
        return None
