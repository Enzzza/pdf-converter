import os
import glob
import concurrent.futures
import time
 
os.environ['OMP_THREAD_LIMIT'] = '1'
def main():
    path = "test"
    if os.path.isdir(path) == 1:
        out_dir = "ocr_results//"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
 
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
            image_list = glob.glob(path+"\\*.png")
            for img_path,out_file in zip(image_list,executor.map(ocr,image_list)):
                print(img_path.split("\\")[-1],',',out_file,', processed')
 
if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end-start)