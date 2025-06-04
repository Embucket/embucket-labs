import logging
from typing import List
from glob import glob


def check_macro_name_to_file_name(files: List):
    for filename in files:
        with open(filename, "r") as stream:
            contents = stream.read()
            error_message = "Macro naming error in " + filename
            realfilename = filename.split("/")[-1].split(".sql")[0]
            logging.info(f"Processing {realfilename}."),
            isolating_macro = contents.split("macro ")[1].split("(")[0]
            if realfilename == isolating_macro in contents:
                logging.info(f"{filename} passes! 🎉")
                pass
            elif realfilename in ("alter_warehouse", "dbt_logging"):
                logging.info(f"Skipping {filename} 👀")
                pass
            else:
                raise ValueError(error_message)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Stating macro name check... ")

    files = glob("macros/**/*.sql", recursive=True)
    logging.info("Files collected... ")

    check_macro_name_to_file_name(files)

    logging.info("All macros match their filenames! 🥂")
