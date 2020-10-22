import os
import glob
from pathlib import Path
from DbConnector import DbConnector
import datetime

def get_label_path(path):
    splitted_path = path.split('/')
    if len(splitted_path) < 2:
        raise Exception('Something wrong with label path')

    for i in range(2):
        splitted_path.pop()
    user_path = '/'.join(splitted_path)
    return user_path + '/labels.txt'

def get_label(path, track_point_start_date, track_point_end_date):
    with open(path, 'r') as file:
        lines = file.readlines()

        for i in range(1, len(lines)):
            splitted_line = lines[i].split()
            if len(splitted_line) != 5:
                raise Exception('Label line should have 5 entries')

            label_start_date = (splitted_line[0] + '+' + splitted_line[1]).replace('/', '-')
            label_end_date = (splitted_line[2] + '+' + splitted_line[3]).replace('/', '-')
            label = splitted_line[4]
            if label_start_date == track_point_start_date and label_end_date == track_point_end_date:
                return label
        
        return False

def get_date_and_time(track_point_line):
    dateAndTime = track_point_line.split(',')[-2:]
    return '+'.join(dateAndTime)

def get_date_object(date_string):
    year = int(date_string[:4])
    month = int(date_string[5:7])
    day = int(date_string[8:10])
    hour = int(date_string[11:13])
    minute = int(date_string[14:16])
    second = int(date_string[17:19])
    date_obj = datetime.datetime(year, month, day, hour, minute, second)
    return date_obj

def get_track_point(track_point_line, id, activity_id):
    track_point = {}
    splitted_track_point = track_point_line.split(',')
    if len(splitted_track_point) != 7:
        raise Exception('Track point line should have 7 entries')

    track_point = {
        '_id': id,
        'lat': float(splitted_track_point[0]),
        'lon': float(splitted_track_point[1]),
        'altitude': int(float(splitted_track_point[3])),
        'date_days': float(splitted_track_point[4]),
        'date_time': get_date_object(splitted_track_point[5] + '+' + splitted_track_point[6]),
        'activity_id': activity_id 
    }
    return track_point

def main():
    activity_id_counter = 1
    track_point_id_counter = 1
    db = DbConnector().db
    path = Path(__file__).parent.parent.absolute()

    for filepath in glob.iglob(r'%s/**/*.plt' % path, recursive=True):
        with open(filepath, 'r') as file:
            user_id = file.name.split('/')[-3]
            activity = {'_id': activity_id_counter, 'user_id': user_id}
            track_points = []

            lines = file.readlines()

            if len(lines) > 2500:
                continue

            first_track_point = lines[6].strip()
            start_date_time = get_date_and_time(first_track_point)
            start_date_time_object = get_date_object(start_date_time)
            activity['start_date_time'] = start_date_time_object

            last_track_point = lines[-1].strip()
            end_date_time = get_date_and_time(last_track_point)
            end_date_time_object = get_date_object(end_date_time)
            activity['end_date_time'] = end_date_time_object

            label_path = get_label_path(file.name)
            is_labeled = os.path.isfile(label_path)
            if is_labeled:
                label = get_label(label_path, start_date_time, end_date_time)
                if label:
                    activity['transportation_mode'] = label

            for i in range(6, len(lines)):
                track_point_line = lines[i].strip()
                track_point = get_track_point(track_point_line, track_point_id_counter, activity_id_counter)
                track_points.append(track_point)
                track_point_id_counter += 1

            activity_id_counter += 1

            db['Activity'].insert_one(activity)
            db['TrackPoint'].insert_many(track_points)
            print('FERDIG MED FIL %s\n' % activity_id_counter)

def drop():
    db = DbConnector().db
    db['Activity'].drop()
    db['TrackPoint'].drop()
    print('Collections dropped')

if __name__ == '__main__':
    main()
