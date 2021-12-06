rm(list = ls())

# Load packages 
library(data.table)
library(lubridate)
library(openxlsx)
library(gridExtra)
library(ggplot2)
library(stringr)
library(dplyr)

# 0. Import the data
data_0 <- fread("0-data/online_offline_data0.txt")
data_1 <- fread("0-data/online_offline_data1.txt")
data_2<- fread("0-data/online_offline_data2.txt")
data_3<- fread("0-data/online_offline_data3.txt")
data_4 <- fread("0-data/online_offline_data4.txt")
data_all <- rbind(data_0, data_1, data_2, data_3, data_4)


# 1. Import the data network (lines, type, colors)
network_lines <- fread("~/data-mining-project/network_lines.csv")
network_lines <- network_lines[, .(LIGNE, COLOR_HEX)]
network_lines[, type_vehicle := substr(LIGNE,4, 4)]
network_lines[, line_id := substr(LIGNE, 1, 3)]
network_lines[, line_id := str_remove(line_id, "^0+")]
network_fin <- unique(network_lines[, .(line_id, type_vehicle, COLOR_HEX)])
network_fin[, line_id := as.integer(line_id)]


# Importing data coordinates
coordinates_stops <- fread("~/Desktop/coordinates_stops.csv")
stops_bxl <- coordinates_stops[, .(alpha_fr, coord_x, coord_y) ]
stops_bxl


# Merging the data
result_join <- 
  as.data.table(left_join(x = data_all, y = network_fin, by = "line_id"))

# 1.Process data
result_join[, ts:= paste0('2021-10-01 ', actual_time)]
result_join[, ts:= as.POSIXct(ts , format = "%Y-%m-%d %H:%M:%OS")]
setkey(result_join, ts)

result_join[, hour := hour(ts)]
result_join[, moments := ifelse(hour >= 0 & hour <= 6, 'night', 
                                       ifelse(hour >=7  & hour <=12, 'morning', 
                                              ifelse(hour >= 13 & hour <= 18, 'afternoon', 'evening')))]

# Computation by Hour
result_join[, avg_speed_hour := mean(speed, na.rm = T), by = c('hour', 'line_id')]
result_join[, avg_delay_hour := mean(delay, na.rm = T), by = c('hour', 'line_id')]

# Computation by Moments
result_join[, avg_speed_moments := mean(speed, na.rm = T), by = c('moments', 'type_vehicle')]
result_join[, avg_delay_moments := mean(delay, na.rm = T), by = c('moments', 'type_vehicle')]

# Computation by type (m, b, t)
result_join[, avg_speed_type := mean(speed, na.rm = T), by = c('hour', 'type_vehicle')]
result_join[, avg_delay_type := mean(speed, na.rm = T), by = c('hour', 'type_vehicle')]


# Unique data 
dt_hour <- unique(result_join[, .(line_id,type_vehicle, hour, avg_speed_hour, avg_delay_hour)])
dt_moments <- unique(result_join[, .(line_id, type_vehicle, hour, moments, avg_speed_moments, avg_delay_moments)])
dt_type <- unique(result_join[, .(line_id, hour, type_vehicle, avg_speed_type, avg_delay_type)])

# Prep. 
dt_hour[, line_id := as.factor(line_id)]
dt_moments[, line_id := as.factor(line_id)]
dt_type[, line_id := as.factor(line_id)]


# Generate report R markdown on aggregated data
rmarkdown::render(file.path("generate_report.Rmd"),
                  output_file = 'report_data_mining.pdf',
                 # output_dir = file.path("reports", "indi_reports"),
                  params = list(dt_hour = dt_hour,  
                                dt_moments = dt_moments, 
                                dt_type = dt_type,
                                stops_bxl = stops_bxl))

#----------------VISUALIZATION SPEED------------------------------------------------------------

## 1. Over the time (no info about location)


# Plot 1: Average hourly speed 

metro <- ggplot(data = dt_hour[type_vehicle == 'm', ], aes(x = hour, y = avg_speed_hour, col = line_id )) + 
  geom_point() +
  geom_line() +
  theme_minimal() +
  ggtitle('What is the average hourly speed of the METRO STIB lines?')

bus <- ggplot(data = dt_hour[type_vehicle == 'b', ], aes(x = hour, y = avg_speed, col = line_id, group = line_id)) + 
  geom_point() +
  geom_line() +
  theme_minimal() +
  ggtitle('What is the average hourly speed of the BUS STIB lines?')


tram <- ggplot(data = dt_hour[type_vehicle == 't', ], aes(x = hour, y = avg_speed, col = line_id, group = line_id )) + 
  geom_point() +
  geom_line() +
  theme_minimal() +
  ggtitle('What is the average hourly speed of the TRAM STIB lines?')


grid.arrange(metro, bus, tram , nrow = 3)


# Plot 2: Plot average speed by moments of the day 
dt_moments[, line_id := as.factor(line_id)]
dt_moments[, moments := factor(moments, level = c('morning', 'afternoon', 'evening', 'night'))]

ggplot(data = dt_moments, 
       aes(x = moments, y = avg_speed_moments, col = type_vehicle, group = type_vehicle ))  +
  geom_point() +
  geom_line() + 
  theme_minimal() + 
  ggtitle('Average speed by moments: all lines')



ggplot(data = dt_moments, 
       aes(x = moments, y = avg_delay_moments, col = type_vehicle, group = type_vehicle ))  +
  geom_point() +
  geom_line() + 
  theme_minimal() + 
  ggtitle('Average delay moments: all lines')




# Comment 1: too much information and too much line 
# Approach 1: Split the lines by type (METRO, BUS, TRAM)

# plot type 1
ggplot(data = dt_type, aes(x = hour, y = avg_speed_type, col = type_vehicle, group = type_vehicle)) +
  geom_point() +
  geom_line() +
  theme_minimal() + 
  ggtitle('Average hourly speed by typology (BUS/METRO/TRAM)')


# plot type 2
ggplot(data = dt_type, aes(x = hour, y = avg_speed_type, col = type_vehicle, group = type_vehicle)) +
  geom_point() +
  geom_line() +
  theme_minimal() + 
  facet_wrap( type_vehicle ~ .) +
  ggtitle('Average hourly speed by typology (BUS/METRO/TRAM)')



#------------- DENSITY DISTRIBUTION 
#------------- STATISTICS 

#------------- w.r.t to the NETWORK Plot Map Brussels 



# Plotting Bruxelles Stops
library(data.table)


pj <- project(xy, proj4string, inverse=TRUE)
latlon <- data.frame(lat=pj$y, lon=pj$x)
print(latlon)








# Bru airport
library(leaflet)
leaflet() %>%
  addTiles() %>%  # Add default OpenStreetMap map tiles
  addMarkers(lng=4.483998064, lat=50.90082973, popup="brussels airport")








#------------- PART 2: "VISUALIZATION DELAY"  ---------------------------------------------------------














