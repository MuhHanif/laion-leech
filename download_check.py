from download import *
import pandas as pd
import os
from threading import Thread
import multiprocessing
from PIL import Image
import PIL
import os


# folder_dir = "/root/laion_downloader"
# dataset = "laion_aesthetics_1024_33M_1.parquet"
# laion_dataset = pd.read_parquet(os.path.join(folder_dir, dataset))

# read real db export and training data


# number_of_workers = 238
# thread_per_worker = 10
# save_path = "/root/project/dataset"


# inner join
def download(dataset: pd.DataFrame, save_path: str):
    for index, sample in dataset.iterrows():
        # print(index)

        attempt = 0
        while attempt < 3:
            try:
                # generate url

                url = sample["URL"]

                user_agent_message = (
                    f"i need to rebuild my dataset for training, "
                    f"please let me know if this bot is pulling data to fast "
                )

                # download image as PIL object
                image = stream_image(
                    url, user_agent=user_agent_message, threshold_size=0
                )

                image = rescale_image(image, 1024)

                save_webp_without_alpha(
                    image,
                    os.path.join(save_path, f"{sample['hash']}.webp"),
                    quality=70,
                )

                break
            except:
                print(f"failed downloading {index} ... retrying {attempt}/3")
                attempt += 1
        else:
            print(f"download attempt exceeded skipping {index}")
            continue


def multithread_download(
    df: pd.DataFrame, save_path: str, number_of_workers: int = 10
) -> None:
    # function tp be executed as threads
    split_df = split_dataframe(df, number_of_workers)

    threads = []
    for df in split_df:
        thread_instance = Thread(target=download, args=[df, save_path])
        threads.append(thread_instance)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def check_error(filename: str) -> list:
    list_broken_image = []
    try:
        im = Image.open(filename)
        im.verify()
        im.close()
        im = Image.open(filename)
        im.transpose(PIL.Image.FLIP_LEFT_RIGHT)
        im.close()
    except Exception as e:
        print(f"image error {filename}: {e}")
        list_broken_image.append(filename)
    return list_broken_image


def main(n):
    # read real db export and training data

    number_of_workers = 94
    thread_per_worker = 10
    save_path = f"/home/user/data_dump/laion"
    print(save_path)

    # split_df = split_dataframe(laion_dataset, number_of_workers)

    # args = [(df,) + (save_path,) + (thread_per_worker,) for df in split_df]

    # with multiprocessing.Pool(processes=number_of_workers) as pool:
    #     results = pool.starmap(multithread_download, args)
    # print(results)

    # check integrity
    list_image = os.listdir(save_path)
    list_image = [os.path.join(save_path, image) for image in list_image]

    with multiprocessing.Pool(processes=number_of_workers) as pool:
        results = pool.map(check_error, list_image)
        # print(results)

    flat_list = []
    for sublist in results:
        for element in sublist:
            flat_list.append(element)

    broken_image = [text.split("/")[-1] for text in flat_list]
    broken_image = [md5.split(".")[0] for md5 in broken_image]
    print(broken_image)


for x in range(1, 2):
    main(x)
