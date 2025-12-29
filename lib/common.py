import subprocess, tarfile, os, re
import paramiko
import logging
from logging.handlers import RotatingFileHandler
import locale
from typing import Tuple, Optional, Dict, Union

def run_cmd(
    command: Union[str, list], 
    cwd: Optional[str] = None, 
    shell: bool = True, 
    timeout: int = 15, 
    check: bool = False, 
    verbose: bool = True,
    env: Optional[Dict[str, str]] = None,
    encoding: Optional[str] = None
) -> Tuple[int, str]:
    """
    执行命令
    参数:
        command: 命令
        cwd: 工作目录
        shell: 是否使用shell执行
        timeout: 超时时间
        check: 是否检查返回码
        verbose: 是否打印输出
        env: 环境变量
        encoding: 编码
    返回:
        返回码，输出
    异常:
        subprocess.CalledProcessError
    """
    cwd = cwd or os.getcwd()
    
    if encoding is None:
        encoding = locale.getpreferredencoding()
    
    if verbose:
        print(f"Running: {command} | CWD: {cwd}")

    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            shell=shell,
            timeout=timeout,
            capture_output=True, # 捕获输出
            text=True,           # 自动解码
            encoding=encoding,
            errors='ignore',     # 忽略解码错误
            env=env or os.environ.copy()
        )
        
        output = result.stdout + result.stderr
        
        if verbose:
            print(f"Return Code: {result.returncode}")
            if output.strip():
                print(f"Output:\n{output.strip()}")

        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, command, output=output)

        return result.returncode, output

    except subprocess.TimeoutExpired as e:
        if verbose:
            print(f"Command timed out after {timeout}s")
        raise e
    except Exception as e:
        if verbose:
            print(f"Execution failed: {e}")
        raise e

def get_local_ip():
    """
    获取本机IP
    """ 
    cmd = "ip -4 addr show"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, text=True).stdout
    matches = re.findall(r'inet (\d+\.\d+\.\d+\.\d+)/\d+', result)
    for ip in matches:
        if not ip.startswith("127."):
            return ip
    return None

def make_targz_one_by_one(
        source_dir: str, 
        output_filename: str, 
        include_ext=None
) -> None:
    """
    使用 tar.gz 压缩文件进行打包
    参数:
        source_dir: 源目录
        output_filename: 输出文件名
        include_ext: 包含的扩展名
    """
    with tarfile.open(output_filename, "w:gz") as tar:
        for root, _, files in os.walk(source_dir):
            for file in files:
                if include_ext and not file.endswith(include_ext):
                    continue  # skip non-matching files
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, start=source_dir)
                tar.add(full_path, arcname=arcname)
                total_files += 1
    print(f"Created tar.gz: {output_filename} ")

def remote_operate(    
        server_ip: str, 
        local_script_path: str,
        local_output_dir: str,
        remote_script_dir: str,
        remote_output_path: str,
        remote_output_filename: str,
        ssh_port=22, 
        ssh_user: str = "root",
        ssh_password: str = "root"
) -> None:
    """
    远程服务器操作

    参数:
        server_ip: 服务器 IP
        local_script_path: 本地脚本路径
        local_output_dir: 本地输出目录
        remote_script_dir: 远程脚本目录
        remote_output_path: 远程输出目录
        remote_output_filename: 远程输出文件名（需要跟脚本输出文件名一致）
        ssh_port: SSH 端口
        ssh_user: SSH 用户名
        ssh_password: SSH 密码
    返回:
        None
    异常:

    """
    
    remote_script_name = os.path.basename(local_script_path)  
    remote_script_path = "{}{}".format(remote_script_dir, remote_script_name)  
    remote_output_path = "{}{}_{}".format(remote_output_path, server_ip, remote_output_filename)
    
    os.makedirs(local_output_dir, exist_ok=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  

    try:
        print("\n------------ Connect to the server {} ------------".format(server_ip))
        ssh.connect(
            hostname=server_ip,
            port=ssh_port,
            username=ssh_user,
            password=ssh_password,
            timeout=30  
        )

        # 上传本地脚本到远程服务器
        print("Upload the script to {}:{}".format(server_ip, remote_script_dir))
        sftp = ssh.open_sftp()  
        try:
            sftp.stat(remote_script_dir)  
        except FileNotFoundError:
            ssh.exec_command("mkdir -p {}".format(remote_script_dir))
        sftp.put(local_script_path, remote_script_path)
        sftp.close()  # 关闭SFTP会话，释放资源

        # 远程执行脚本
        print("Executing the script on {}...".format(server_ip))
        exec_cmd = "python3 {}".format(remote_script_path)
        # 执行命令，设置3分钟超时
        stdin, stdout, stderr = ssh.exec_command(exec_cmd, timeout=180)

        # 实时打印远程脚本的输出信息
        for line in stdout:
            print("[{}] Script output:{}".format(server_ip, line.strip()))
        # 捕获脚本执行的错误信息
        error_msg = stderr.read().decode()
        if error_msg:
            print("[{}] Execution error:{}".format(server_ip, error_msg))
            return  

        # 检查远程文件是否存在
        check_cmd = "if [ -f {} ]; then echo 'exists'; else echo 'no'; fi".format(remote_output_path)
        stdin, stdout, stderr = ssh.exec_command(check_cmd)
        file_exists = stdout.read().decode().strip()  

        # 从远程取出下载到本地的文件路径
        local_file_path = os.path.join(local_output_dir, os.path.basename(remote_output_path))

        if file_exists == "exists":
            # 文件存在，执行下载
            print("Download the output file to the local: {}".format(local_file_path))
            sftp = ssh.open_sftp()
            sftp.get(remote_output_path, local_file_path) 
            sftp.close()
        else:
            # 文件不存在，打印提示并跳过
            print("[{}] Output file {} not found, skipping download".format(server_ip, local_file_path))

    except Exception as e:
        print("[{}] Operation failed: {}".format(server_ip, str(e)))
    finally:
        # 关闭SSH连接
        if ssh.get_transport() and ssh.get_transport().is_active():
            ssh.close()
        print("------------ Disconnect from {} ------------\n".format(server_ip))

def log_setup(
        name: str, 
        log_file: str, 
        level=logging.INFO
)-> logging.Logger:
    """
    日志设置

    参数:
        name: 日志名称
        log_file: 日志文件路径
        level: 日志级别
    返回:
        logger: 日志对象
    """
    
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler = RotatingFileHandler(log_file, maxBytes=100*1024*1024, backupCount=3, encoding="utf-8")
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False

    return logger
