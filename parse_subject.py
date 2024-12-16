import pydicom
import os
import argparse
import numpy as np
from datetime import datetime, timedelta
import json
from concurrent.futures import ProcessPoolExecutor


# # convert s string from YYYYMMDDHHMMSS.XXX000 to YYYYMMDDHHMMSSXXX
# def get_time_stamp_string(input_str):
#     if input_str == "":
#         return ""
#     time_stamp = input_str.split(".")[0]
#     millisecond = input_str.split(".")[1]

#     # trim the last tree 0s
#     millisecond = millisecond[0:3]

#     return time_stamp + millisecond

# def get_time_stamp_value(input_str):
#     # convert a YYYYMMDDHHMMSS.XXX000 to a number
#     ts_string = get_time_stamp_string(input_str)

#     return int(ts_string)


# input 0: initial timestamp YYYYMMDDHHMMSS.XXX000 (str), 
# input 1: frame time float value in milliseconds (float)
# return: new timestamp in the format of YYYYMMDDHHMMSSXXX (str)
def get_current_time_stamp(first_timestamp_str, frame_time_float):
    # convert frame_time_float from pydicom.valuerep.DSfloat to float
    frame_time_float = float(frame_time_float)
    assert type(first_timestamp_str) == str, "First timestamp must be a string"
    assert type(frame_time_float) == float, "Frame time must be a float"
    fmt = "%Y%m%d%H%M%S.%f"
    # Convert the first timestamp string to a datetime object
    first_timestamp = datetime.strptime(first_timestamp_str, fmt)
    # Calculate the time delta from the frame time in milliseconds
    frame_time_delta = timedelta(milliseconds=frame_time_float)
    # Add the two times together
    new_timestamp = first_timestamp + frame_time_delta
    # Format the new timestamp as YYYYMMDDHHMMSS.XXX000
    new_timestamp_str = new_timestamp.strftime("%Y%m%d%H%M%S.") + f"{new_timestamp.microsecond // 1000:03d}000"
    return new_timestamp_str

def get_time_stamp_list(initial_timestamp, frame_time_vector):
    time_stamp_list = []
    for i in range(len(frame_time_vector)):
        if i == 0:
            time_stamp = get_current_time_stamp(initial_timestamp, frame_time_vector[i])
        else:  
            time_stamp = get_current_time_stamp(time_stamp_list[i-1], frame_time_vector[i])
        # time_stamp is a str
        assert type(time_stamp) == str, "Time stamp must be a string"
        time_stamp_list.append(time_stamp)
    return time_stamp_list

def convert_time_stamp_format(time_stamp):
    # for each element, convert from YYYYMMDDHHMMSS.XXX000 to YYYYMMDDHHMMSSXXX
    for i in range(len(time_stamp)):
        original_time_stamp = time_stamp[i]
        time_stamp[i] = original_time_stamp.split(".")[0] + original_time_stamp.split(".")[1][0:3]
        # print("Converting from: ", original_time_stamp, " to ", time_stamp[i])
    return time_stamp
    
def parse_frame_data(raw_data):
    # raw_data shape: (num_frames, dim_x, dim_y, c=3)
    num_frames, dim_x, dim_y, c = raw_data.shape

    # parse first channel as 2D time series W x H x T
    time_series_data = np.zeros((dim_x, dim_y, num_frames))
    for i in range(num_frames):
        time_series_data[:, :, i] = raw_data[i, :, :, 0]

    # import matplotlib.pyplot as plt
    # # plot the first frame
    # plt.imshow(time_series_data[:, :, 0], cmap='gray')

    # # pause the program util a key is pressed
    # plt.show()

    return time_series_data

# input: path of a single dicom file
# return: a list of objects, each object contains "time_stamp" (YYYYMMDDHHMMSSXXX) and "data" (3xwxh, 3D numpy array)
def parse_dicom(dicom_file):
    one_loop = []


    ds = pydicom.dcmread(dicom_file)
    # get Acquisition Date Time
    acquisition_datetime_str = ds.get('AcquisitionDateTime')
    # print(acquisition_datetime_str) # format: YYYYMMDDHHMMSS.XXXXXX
    
    # get Frames Time Vector (Tag ID (0018,1065))
    frame_time_vector = ds.get('FrameTimeVector')
    
    # get image data
    raw_data = ds.pixel_array # array shape: (num_frames, dim_x, dim_y, c=3)

    # parse frame data as an array of 2D image T => W x H
    time_series_array = parse_frame_data(raw_data) 

    # get X and Y pixel spacing
    ultrasound_regions = ds.get((0x0018,0x6011), [])
    for region in ultrasound_regions:
        physical_delta_x = region.get((0x0018, 0x602C), None)
        physical_delta_y = region.get((0x0018, 0x602E), None)
        if physical_delta_x is not None and physical_delta_y is not None:
            # convert to float
            physical_delta_x = float(physical_delta_x.value)
            physical_delta_y = float(physical_delta_y.value)
            break

    # get the list of time stamps
    time_stamp_list = get_time_stamp_list(acquisition_datetime_str, frame_time_vector)
    # convert time stamp format
    time_stamp_list = convert_time_stamp_format(time_stamp_list)

    # get rows and columns
    rows = ds.get((0x0028, 0x0010), None)
    columns = ds.get((0x0028, 0x0011), None)

    for i in range(len(time_stamp_list)):
        one_frame = {}
        one_frame['time_stamp'] = time_stamp_list[i]
        one_frame['data'] = time_series_array[:, :, i]
        one_frame['physical_delta_x'] = physical_delta_x
        one_frame['physical_delta_y'] = physical_delta_y
        one_frame['dim_y'] = int(rows.value)
        one_frame['dim_x'] = int(columns.value)
        one_loop.append(one_frame)

    return one_loop


def write_json_file(frame, dirout_loop):
    time_stamp = frame['time_stamp']
    data = frame['data']
    physical_delta_x = frame['physical_delta_x']
    physical_delta_y = frame['physical_delta_y']
    dim_y = frame['dim_y']
    dim_x = frame['dim_x']

    # create a json file
    json_file = os.path.join(dirout_loop, time_stamp + ".json")

    # serialize the np array to a string to be put in json
    data_str = json.dumps(data.tolist())

    # create a json file
    with open(json_file, "w") as f:
        json.dump({
            "time_stamp": time_stamp,
            "physical_delta_x": physical_delta_x,
            "physical_delta_y": physical_delta_y,
            "dim_x": dim_x,
            "dim_y": dim_y,
            "data": data_str
        }, f, indent=4)


def generate_loop_files(loop, dirout_loop, num_workers=None):
    with ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(write_json_file, loop, [dirout_loop] * len(loop))


# parse all in the directory, for each file, call parse_dicom
def process_dicom_file(dicom_file, dirout_subject):
    print(f"Processing {dicom_file}")
    try:
        one_loop = parse_dicom(dicom_file)
        first_timestamp = one_loop[0]['time_stamp']
        dirout_loop = os.path.join(dirout_subject, first_timestamp)
        os.makedirs(dirout_loop, exist_ok=True)
        generate_loop_files(one_loop, dirout_loop)
        return first_timestamp
    except Exception as e:
        print(f"Error processing {dicom_file}: {e}")
        return 'error'


def parse_subject(dir_subject, dir_out):
    print(dir_subject)

    dirout_subject = ""

    for root, dirs, files in os.walk(dir_subject):
        dicom_files = [os.path.join(root, file) for file in files if file.endswith(".dcm")]
        
        if dicom_files:
            # Process the first file to get the dirout_subject
            first_dicom_file = dicom_files[0]
            one_loop = parse_dicom(first_dicom_file)
            first_timestamp = one_loop[0]['time_stamp']
            dirout_subject = os.path.join(dir_out, f"__temp_{first_timestamp}") # temporary, will be renamed
            os.makedirs(dirout_subject, exist_ok=True)

            # Process the rest of the files in parallel
            with ProcessPoolExecutor(max_workers = 8) as executor:
                try:
                    results = executor.map(process_dicom_file, dicom_files, [dirout_subject] * len(dicom_files))
                except Exception as e:
                    print(f"Error in ProcessPoolExecutor: {e}")

    global_min_time_stamp = float('inf')
    global_min_time_stamp_str = ""

    for min_time_stamp in results:
        if float(min_time_stamp) < global_min_time_stamp:
            global_min_time_stamp = float(min_time_stamp)
            global_min_time_stamp_str = min_time_stamp
    
    # print("Global Min Time Stamp Str: ", global_min_time_stamp_str)

    new_dirout_subject = os.path.join(dir_out, global_min_time_stamp_str)
    os.rename(dirout_subject, new_dirout_subject)


def main():
    parser = argparse.ArgumentParser(description='Parse DICOM files.')

    # subject input directry contains dicom files
    parser.add_argument('dir_subject', type=str, help='Directory containing DICOM files')
    
    # output directory argument
    parser.add_argument('dir_output', type=str, help='Directory to save the output')
    
    args = parser.parse_args()
    
    parse_subject(args.dir_subject, args.dir_output)

if __name__ == "__main__":
    main()