from datetime import datetime, timedelta

def format_tle_line(stl_str: str) -> str:
    """
    格式化TLE字符串，确保每行长度为69个字符，并删除line0
    :param stl_str: TLE字符串
    :return: 如果格式正确，返回格式化后的字符串；否则返回None
    """

    # 按换行符提取每行
    lines = stl_str.splitlines()
    if(len(lines) == 2):
        line1 = lines[0].strip()
        line2 = lines[1].strip()
    elif(len(lines) == 3):
        line1 = lines[1].strip()
        line2 = lines[2].strip()
    else:
        return None
    
    # 检查格式是否正确
    if(len(line1) == 69 and 
       len(line2) == 69 and
       line1.startswith('1 ') and
       line2.startswith('2 ')):
        return f"{line1}\n{line2}"
    else:
        return None
    
def check_tle_format(tle_str : str) -> bool:
    """
    检查TLE字符串格式是否正确
    :param tle_str: TLE字符串
    :return: 如果格式正确，返回True；否则返回False
    """

    # 使用format_tle_line进行检查
    formatted_tle = format_tle_line(tle_str)
    if formatted_tle is not None:
        return True
    else:
        return False

def extract_tle_from_text(tle_text: str) -> list[str]:
    """
    从文本中提取TLE数据
    :param tle_text: 包含TLE数据的字符串
    :return: 返回一个包含TLE数据的列表，每个元素是一个TLE字符串
    """
    lines = tle_text.splitlines()
    tle_lines = []
    for i in range(len(lines) - 1):
        line1 = lines[i].strip()
        line2 = lines[i + 1].strip()
        if (line1.startswith('1 ') and len(line1) == 69 and
            line2.startswith('2 ') and len(line2) == 69):
            tle_lines.append(f"{line1}\n{line2}")
            tle_str = f"{line1}\n{line2}"
            if(check_tle_format(tle_str)):
                tle_lines.append(tle_str)
    return tle_lines

def get_info_from_tle(tle_str: str) -> tuple[str, datetime]:
    """
    从TLE字符串中提取信息
    :param tle_str: TLE字符串
    :return: 返回一个字典，包含TLE的相关信息
    """
    lines = format_tle_line(tle_str).splitlines()
    if len(lines) < 2:
        return {}

    line1 = lines[0].strip()

    norad_cat_id = line1[2:7].strip()
    year = line1[18:20].strip()
    if year.isdigit():
        year = int(year)
        if year < 57:  # 2000年之前
            year += 2000
        else:  # 2000年及之后
            year += 1900
    else:
        year = 1900  # 默认值

    day_of_year = line1[20:32].strip()
    try:
        day_of_year = float(day_of_year)
    except ValueError:
        day_of_year = 0

    

    # 根据year和day_of_year计算datetime格式的tle_time
    if year is not None and day_of_year is not None:
        tle_time = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    else:
        tle_time = None

    return norad_cat_id, tle_time


if __name__ == "__main__":
    # 测试代码
    test_tle = "FIA Radar 1\n1 37162U 10046A   25157.13869119 0.00000000  00000-0  00000-0 0    01\n2 37162 123.0185 209.3664 0006064  37.2960 322.7040 13.41447413    04"
    formatted_tle = format_tle_line(test_tle)
    print("Formatted TLE:")
    print(formatted_tle)

    print("info from TLE:")
    norad, tle_time = get_info_from_tle(test_tle)
    print(f"NORAD: {norad}")
    print(f"TLE Time: {tle_time}")