import os


class SkipEntryException(Exception):
    """用户选择跳过当前entry处理的异常"""
    pass


def select_output_excel(parent_dir: str, entry_name: str) -> str:
    """
    选择输出Excel文件名，避免覆盖。
    格式: output_{entry_name}.xlsx, output_{entry_name}_2.xlsx, ...
    """
    output_excel = os.path.join(parent_dir, f"output_{entry_name}.xlsx")
    idx = 2
    while os.path.exists(output_excel):
        output_excel = os.path.join(
            parent_dir, f"output_{entry_name}_{idx}.xlsx"
        )
        idx += 1
    return output_excel


def select_excel_from_matches(matches: list, entry_dir: str,
                              prompt_msg: str) -> str:
    """
    如果有多个匹配项，引导用户选择；如果没有则提示输入；否则直接返回唯一项。
    """
    if not matches:
        # 用户输入的一定是绝对路径，直接返回
        fname = prompt_for_excel(entry_dir, prompt_msg)
        print(f"[INFO] 选中文件绝对路径: {fname}")
        return fname
    if len(matches) == 1:
        # 对 matches 中的路径进行 join
        abs_path = os.path.join(entry_dir, matches[0])
        print(f"[INFO] 选中文件绝对路径: {abs_path}")
        return abs_path
    while True:
        print(f"{prompt_msg}（当前目录: {entry_dir}）")
        print("检测到多个匹配文件，请选择编号：")
        for idx, fname in enumerate(matches, 1):
            print(f"{idx}: {fname}")
        print("提示：输入 'skip' 可跳过当前entry的处理")
        sel = input("请输入编号: ").strip()

        # 检查是否要跳过
        if sel.lower() == 'skip':
            raise SkipEntryException(
                f"用户选择跳过entry: {os.path.basename(entry_dir)}")

        if sel.isdigit():
            idx = int(sel)
            if 1 <= idx <= len(matches):
                # 对 matches 中的路径进行 join
                abs_path = os.path.join(entry_dir, matches[idx - 1])
                print(f"[INFO] 选中文件绝对路径: {abs_path}")
                return abs_path
        print("输入无效，请重新输入编号。")


def prompt_for_excel(entry_dir: str, prompt_msg: str) -> str:
    """
    若未找到Excel文件，则提示用户输入文件名，并去除首尾空格。
    用户可以输入'skip'来跳过当前entry的处理。
    """
    print(f"{prompt_msg}（当前目录: {entry_dir}）")
    print("提示：输入 'skip' 可跳过当前entry的处理")
    fname = input("请输入文件名（含扩展名）：").strip()

    # 检查是否要跳过
    if fname.lower() == 'skip':
        raise SkipEntryException(f"用户选择跳过entry: {os.path.basename(entry_dir)}")

    # 去除首尾引号和空格
    fname = fname.strip('"\' ').strip()
    return fname
