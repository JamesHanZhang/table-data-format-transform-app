from basic_operation.basic_io_operation import IoMethods
from analysis_modules.default_properties.project_properties import PROJECT_NAME
def get_folder_path_under_project(folder):
    parent_path = IoMethods.search_upward_path(PROJECT_NAME)
    full_path = IoMethods.join_path(parent_path,folder)
    return full_path

SYS_LOG_PATH = get_folder_path_under_project("log")
RESOURCES_PATH = get_folder_path_under_project("resources")
INPUT_PATH = get_folder_path_under_project("input_dataset")
OUTPUT_PATH = get_folder_path_under_project("output_dataset")