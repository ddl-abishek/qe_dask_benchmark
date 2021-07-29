def convert_date(date): 
    date = str(date)
    months_dict = {'01':31,'02':{'leap':29,'normal':28},'03':31,'04':30,'05':31,
                  '06':30, '07':31, '08':31,'09':30,'10':31,'11':30,'12':31}
    
    if len(date) >= 8:
        if len(date) > 8: # handling exceptions for  2015 data
            date = date.split('.')[0]
            date = date.split(' ')[0].split('/')[-1]
        day_ext = int(date[-2:])
        month_ext = date[-4:-2]
        year = date[0:4]
        day_of_year = 0            
        for month in months_dict.keys():
            if month == month_ext:
                break
            if month == '02':
                if int(year)%4 == 0:
                    day_of_year += months_dict['02']['leap']
                else:
                    day_of_year += months_dict['02']['normal']
            else:
                day_of_year += months_dict[month]
        day_of_year += day_ext

        return day_of_year