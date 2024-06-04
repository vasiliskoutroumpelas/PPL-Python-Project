import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import messagebox
import mysql.connector
from sqlalchemy import create_engine

df = pd.read_csv('hotel_booking.csv')

df['total_nights'] = df[['stays_in_week_nights', 'stays_in_weekend_nights']].sum(axis=1)

df['arrival_season'] = df['arrival_date_month'].apply(
    lambda x: 'Winter' if x in ['December', 'January', 'February'] else 'Spring' if x in ['March', 'April', 'May']
    else 'Summer' if x in ['June', 'July', 'August'] else 'Autumn')

df['traveller_type'] = df.apply(
    lambda x: 'Family' if x['adults'] > 0 and (x['children'] > 0 or x['babies'] > 0) else 'Couple' if x['adults'] > 1 else 'Single',
    axis=1)

# Define the orders for months and seasons
month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
season_order = ['Winter', 'Spring', 'Summer', 'Autumn']
alphabet_order = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Peristeri03.",
  database="hotel_booking_analysis"
)

mycursor = mydb.cursor()

engine = create_engine("mysql+mysqlconnector://root:Peristeri03.@localhost/hotel_booking_analysis")

mycursor.execute("CREATE TABLE IF NOT EXISTS basic_stats (hotel VARCHAR(255) PRIMARY KEY, mean_nights FLOAT, cancel_rate FLOAT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS booking_distribution_month (arrival_date_month VARCHAR(255) PRIMARY KEY, count INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS booking_distribution_season (arrival_season VARCHAR(255) PRIMARY KEY, count INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS room_type_distribution (reserved_room_type VARCHAR(255) PRIMARY KEY, count INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS traveller_type_distribution (traveller_type VARCHAR(255) PRIMARY KEY, count INT)")
mycursor.execute("CREATE TABLE IF NOT EXISTS booking_trends (arrival_date_year INT, arrival_date_month VARCHAR(255), count INT PRIMARY KEY)")
mydb.commit()


def basic_stats(show_message=True):
    hotels = df['hotel'].unique()
    stats = []

    for hotel in hotels:
        hotel_data = df[df['hotel'] == hotel]
        mean_nights = hotel_data['total_nights'].mean()
        cancel_rate = hotel_data['is_canceled'].mean() * 100
        stats.append({'hotel': hotel, 'mean_nights': mean_nights, 'cancel_rate': cancel_rate})

    stats_df = pd.DataFrame(stats)

    if show_message:
        messagebox.showinfo("Βασικά Στατιστικά", "\n".join(
            [
                f"{row['hotel']}: Μέσος Όρος Διανυκτερεύσεων: {row['mean_nights']:.2f}, Ποσοστό Ακυρώσεων: {row['cancel_rate']:.2f}%"
                for _, row in stats_df.iterrows()]))

    return stats_df

def booking_distribution(plot=True):
    months = df['arrival_date_month'].value_counts().reindex(month_order)
    seasons = df['arrival_season'].value_counts().reindex(season_order)

    if plot:
        months.plot(kind='bar')
        plt.title("Κατανομή Κρατήσεων ανά Μήνα")
        plt.xlabel("Μήνας")
        plt.ylabel("Αριθμός Κρατήσεων")
        plt.show()

        seasons.plot(kind='bar')
        plt.title("Κατανομή Κρατήσεων ανά Εποχή")
        plt.xlabel("Εποχή")
        plt.ylabel("Αριθμός Κρατήσεων")
        plt.show()

    return months, seasons

def room_type_distribution(plot=True):
    room_type = df['reserved_room_type'].value_counts().reindex(alphabet_order)
    if plot:
        room_type.plot(kind='bar')
        plt.title("Κατανομή Κρατήσεων ανά Τύπο Δωματίου")
        plt.xlabel("Τύπος Δωματίου")
        plt.ylabel("Αριθμός Κρατήσεων")
        plt.show()
    return room_type

def traveller_type_distribution(plot=True):
    travellers = df['traveller_type'].value_counts()
    if plot:
        travellers.plot(kind='bar')
        plt.title("Κατανομή Κρατήσεων ανά Τύπο Ταξιδιώτη")
        plt.xlabel("Τύπος Ταξιδιώτη")
        plt.ylabel("Αριθμός Κρατήσεων")
        plt.show()
    return travellers

def booking_trends(plot=True):
    bookings_per_year = df.groupby(['arrival_date_year', 'arrival_date_month']).size().unstack().reindex(columns=month_order)
    if plot:
        bookings_per_year.plot(kind='bar')
        plt.title("Κατανομή Κρατήσεων με την πάροδο του χρόνου")
        plt.xlabel("Μήνες κάθε χρόνου")
        plt.ylabel("Αριθμός Κρατήσεων")
        plt.show()
    df1 = df.groupby(['arrival_date_year', 'arrival_date_month']).size()
    df1 = df1.reset_index(name='count')
    return df1


def export_to_csv():
    basic_stats(show_message=False).to_csv('basic_stats.csv', index=False)

    months, seasons = booking_distribution(plot=False)
    months.to_csv('booking_distribution_month.csv')
    seasons.to_csv('booking_distribution_season.csv')

    room_type_distribution(plot=False).to_csv('room_type_distribution.csv')
    traveller_type_distribution(plot=False).to_csv('traveller_type_distribution.csv')
    booking_trends(plot=False).to_csv('booking_trends.csv')

    messagebox.showinfo("Εξαγωγή σε αρχεία CSV", "Επιτυχής εξαγωγή")

def export_to_mysql_tables():
    mycursor.execute("DELETE FROM basic_stats")
    mydb.commit()
    basic_stats(show_message=False).to_sql(name='basic_stats', con=engine, if_exists='append', index=False)

    mycursor.execute("DELETE FROM booking_distribution_month")
    mydb.commit()
    mycursor.execute("DELETE FROM booking_distribution_season")
    mydb.commit()
    months, seasons = booking_distribution(plot=False)
    months.to_sql(name='booking_distribution_month', con=engine, if_exists='append', index=True)
    seasons.to_sql(name='booking_distribution_season', con=engine, if_exists='append', index=True)

    mycursor.execute("DELETE FROM room_type_distribution")
    mydb.commit()
    room_type_distribution(plot=False).to_sql(name='room_type_distribution', con=engine, if_exists='append', index=True)

    mycursor.execute("DELETE FROM traveller_type_distribution")
    mydb.commit()
    traveller_type_distribution(plot=False).to_sql(name='traveller_type_distribution', con=engine, if_exists='append', index=True)

    mycursor.execute("DELETE FROM booking_trends")
    mydb.commit()
    booking_trends(plot=False).to_sql(name='booking_trends', con=engine, if_exists='append', index=False)

    messagebox.showinfo("Εξαγωγή σε πίνακες MySQL", "Επιτυχής εξαγωγή")

root = tk.Tk()
root.title("Hotel Booking Analysis")

frame = tk.Frame(root, padx=100, pady=100)
frame.grid(row=0, column=0)

tk.Button(frame, text="Βασικά Στατιστικά", command=basic_stats).grid(row=0, column=0, padx=10, pady=10)
tk.Button(frame, text="Κατανομή Κρατήσεων", command=booking_distribution).grid(row=1, column=0, padx=10, pady=10)
tk.Button(frame, text="Κατανομή ανά Τύπο Δωματίου", command=room_type_distribution).grid(row=2, column=0, padx=10, pady=10)
tk.Button(frame, text="Κρατήσεις ανά Τύπο Ταξιδιώτη", command=traveller_type_distribution).grid(row=3, column=0, padx=10, pady=10)
tk.Button(frame, text="Τάσεις Κρατήσεων", command=booking_trends).grid(row=4, column=0, padx=10, pady=10)
tk.Button(frame, text="Εξαγωγή σε αρχεία CSV", command=export_to_csv, background='gray').grid(row=6, column=0, padx=10, pady=10)
tk.Button(frame, text="Εξαγωγή σε πίνακες MySQL", command=export_to_mysql_tables, background='gray').grid(row=7, column=0, padx=10, pady=10)

root.mainloop()
