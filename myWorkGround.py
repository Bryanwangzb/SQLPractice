import datetime
import calendar


def generate_year_month_pairs():
    # Get the current date
    # now = datetime.now()
    now = datetime.date.today()

    # Create a list to store (year, month) tuples
    year_month_pairs = []

    # Loop through the past 14 months including the current month
    for i in range(14):
        # Append the (year, month) tuple to the list
        year_month_pairs.append((now.year, now.month))

        # Move to the previous month
        if now.month == 1:
            now = now.replace(year=now.year - 1, month=12, day=1)
        else:
            now = now.replace(month=now.month - 1, day=1)

    # Reverse the list to get it in chronological order
    return year_month_pairs

year_month_pairs = generate_year_month_pairs()
print(year_month_pairs)

for i in range(12):
    print(calendar.monthrange(year_month_pairs[i+2][0], year_month_pairs[i+2][1])[1]) # previouse_month_days
    print(calendar.monthrange(year_month_pairs[i+1][0], year_month_pairs[i+1][1])[1]) # current_month_days
    print(len(calendar.monthcalendar(year_month_pairs[i+2][0], year_month_pairs[i+2][1]))) # previous_month_weeks
    print(len(calendar.monthcalendar(year_month_pairs[i+1][0], year_month_pairs[i+1][1]))) # current_month_weeks
    print(str(year_month_pairs[i+1][0]) + "/" + str(year_month_pairs[i+1][1]) + "/01") # previous_month
    print(str(year_month_pairs[i][0]) + "/" + str(year_month_pairs[i][1]) + "/01") # current_month
    print(str(year_month_pairs[i+1][0]) + "年" + str(year_month_pairs[i+1][1]) + "月") # year_month : column name
    print("*******")

