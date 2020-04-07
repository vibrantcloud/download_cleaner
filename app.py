from pathlib import Path
import pandas as pd
import schedule
import time


class DownloadCleaner:

    """Cleans my messy download folder"""

    today = pd.Timestamp("today").strftime("_%Y_%m_%d")

    def __init__(self, download_folder):

        self.download_folder = Path.home().joinpath(download_folder)

        self.downloads = (download for download in self.download_folder.glob("*.*"))

        self.download_dict = {
            d: pd.Timestamp(d.stat().st_mtime, unit="s") for d in self.downloads
        }

    def move_and_create_files(self):

        """Takes a pathlib object and iterate over it move files.
        if file exists then it will add the current date_time to handle duplicate names.
        the folder structure will be Year (YYYY) -- > Month (January) --> Extension (.zip) -->"""
        # move files to folders.

        for name, time in self.download_dict.items():
            year, month = time.year, time.month_name()
            new_path = Path(self.download_folder).joinpath(
                str(year), month, name.suffix[1:], name.name
            )
            try:
                new_path.parent.mkdir(parents=True, exist_ok=False)

            except FileExistsError:
                pass

            if (new_path).is_file():
                duplicate_file_name = new_path.stem + self.today + new_path.suffix

                name.rename(new_path.parent.joinpath(duplicate_file_name))
            else:
                name.rename(new_path)

    def create_log_file(self):
        """Creates a simple log file based on the Pathlib object."""

        df = pd.DataFrame(
            {
                "file_name": [f.stem for f in self.download_dict.keys()],
                "parent_path": [f.parent for f in self.download_dict.keys()],
                "ext": [f.suffix[1:] for f in self.download_dict.keys()],
                "download_date": [
                    pd.Timestamp(f.stat().st_mtime, unit="s")
                    for f in self.download_dict.keys()
                ],
                "target_path": [
                    Path(k.parent).joinpath(str(v.year), v.month_name(), k.suffix[1:])
                    for k, v in self.download_dict.items()
                ],
            }
        )

        df["move_date"] = pd.Timestamp("today")

        for group, data in df.groupby(["ext"]):
            print(
                f"{data['file_name'].nunique()} Files moved with an extension of {group}"
            )

        if not self.download_folder.joinpath("log_files").is_dir():
            Path.mkdir(
                self.download_folder.joinpath("log_files"), parents=True, exist_ok=False
            )

        if not self.download_folder.joinpath("log_files", "download_log.csv").is_file():

            df.to_csv(
                self.download_folder.joinpath("log_files", "download_log.csv"),
                index=False,
                mode="w",
            )
        else:
            df.to_csv(
                self.download_folder.joinpath("log_files", "download_log.csv"),
                index=False,
                mode="a",
                header=False,
            )


def do_job():
    print("Running download cleaner...")

    download = DownloadCleaner("downloads")
    download.create_log_file()
    download.move_and_create_files()


schedule.every().hour.do(do_job)


while True:
    schedule.run_pending()
    time.sleep(1)
