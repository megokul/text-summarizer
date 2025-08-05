import os
import math

EXCLUDE_DIRS = {'.venv', 'venv', '__pycache__', '.github', '.git', '.idea', '.vscode', 'build', 'dist', '.mypy_cache'}
INCLUDE_YAML_FILES = {'config.yaml', 'params.yaml', 'schema.yaml', 'templates.yaml'}
BASE_OUTPUT_FILE = "project_code_dump_part"
SUMMARY_FILE = "project_code_dump_index.txt"
STRUCTURE_FILE = "project_structure.txt"


def is_valid_directory(dirname):
    return not any(part in EXCLUDE_DIRS for part in dirname.split(os.sep))


def print_directory_tree(start_path: str, indent: str = "", exclude_dirs=None, out_lines=None) -> list:
    if exclude_dirs is None:
        exclude_dirs = EXCLUDE_DIRS
    if out_lines is None:
        out_lines = []

    try:
        items = sorted(os.listdir(start_path))
    except PermissionError:
        return out_lines

    for item in items:
        item_path = os.path.join(start_path, item)
        if os.path.isdir(item_path):
            if item in exclude_dirs:
                continue
            out_lines.append(f"{indent}📁 {item}/")
            print_directory_tree(item_path, indent + "    ", exclude_dirs, out_lines)
        else:
            out_lines.append(f"{indent}📄 {item}")
    return out_lines


def list_target_files(root_dir):
    py_files = []
    yaml_files = []

    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if is_valid_directory(os.path.join(dirpath, d))]
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(full_path, root_dir)

            if filename.endswith('.py'):
                py_files.append((rel_path, full_path))
            elif filename in INCLUDE_YAML_FILES:
                yaml_files.append((rel_path, full_path))

    return sorted(py_files), sorted(yaml_files)


def chunk_list(data, num_chunks):
    chunk_size = math.ceil(len(data) / num_chunks)
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def dump_project_code_in_parts(root_dir='.', num_parts=1):
    py_files, yaml_files = list_target_files(root_dir)
    tree_lines = print_directory_tree(root_dir, out_lines=[])
    total_files = py_files + yaml_files

    if not total_files:
        print("❌ No .py or relevant .yaml files found.")
        return

    # Save project structure once
    with open(STRUCTURE_FILE, 'w', encoding='utf-8') as struct_file:
        struct_file.write(f"📦 Project Structure of: {os.path.abspath(root_dir)}\n\n")
        struct_file.write("\n".join(tree_lines))
    print(f"✅ Project structure saved to: {os.path.abspath(STRUCTURE_FILE)}")

    file_chunks = chunk_list(total_files, num_parts)
    summary_lines = []

    for i, chunk in enumerate(file_chunks, start=1):
        part_filename = f"{BASE_OUTPUT_FILE}{i}.txt"
        with open(part_filename, 'w', encoding='utf-8') as out_file:
            out_file.write(f"--- CODE DUMP | PART {i} of {num_parts} ---\n\n")

            for rel_path, full_path in chunk:
                summary_lines.append(f"{part_filename}: {rel_path}")
                out_file.write(f"\n{'=' * 80}\n")
                file_type = "PY FILE" if rel_path.endswith('.py') else "YAML FILE"
                out_file.write(f"# {file_type}: {rel_path}\n")
                out_file.write(f"{'=' * 80}\n\n")
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        out_file.write(content.strip() + "\n")
                except Exception as e:
                    out_file.write(f"Error reading {rel_path}: {e}\n")

        print(f"✅ Dumped part {i} to: {os.path.abspath(part_filename)}")

    # Write summary file
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write("📄 File-to-Part Mapping\n\n")
        for line in summary_lines:
            f.write(line + "\n")

    print(f"\n📝 Summary index saved to: {os.path.abspath(SUMMARY_FILE)}")


if __name__ == "__main__":
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    try:
        num_parts = int(input("Enter number of parts to split the dump into: ").strip())
        if num_parts < 1:
            raise ValueError
    except ValueError:
        print("❌ Invalid input. Please enter a positive integer.")
    else:
        dump_project_code_in_parts(ROOT_DIR, num_parts)
