#TODO:1) change input json singature to include csv_type before the type of test - Done
# 2) organize the driver_fn to run tests with minimum if/else statements - Done
# 3) present output json is verbose, add a succint version of result (ex: which columns are missing etc.)
# 4) break the concerns into smaller functions (example: make output_json function to output json)
# 5) inside main_Driver fn incldue : detail driver fn and search driver fn - Done
# 6) replace if/else with dictionary: dict.get(x, default) -  Done
# 7) use try, except, else, finally for handling file and json exceptions
# 8) export jupter ntbk script to vscode and push -Done
# 9) run for detail csv - done
# 10) separate concerns in run_fns 
# 11) create json_handler.py with try except statements for parsing input_json

import json
from Run_fns import *


def main():
  with open("input2.json") as f:
      json_data = json.load(f)
    
  output = []
  def run_driver_fn(json_data):
      for file in json_data["runs"]:
          output.append(run_fns(file))
          
  run_driver_fn(json_data)

  run_index = json_data["run_index"]

  # output = """run_index:{0},
  # runs:{1}
  # """.format(run_index, output)

  final_output_dict = dict()

  final_output_dict["run_index"] = run_index
  final_output_dict["runs"] = output

  with open("output.json", "w") as outfile:
      json.dump(final_output_dict, outfile, indent = 4)


if __name__ == "__main__":
    main()