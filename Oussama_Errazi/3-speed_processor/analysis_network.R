###### Analysis on the speed vehicle: STIB PROJECT #############
rm(list = ls())

# Load Packages
library(data.table)
library(ggplot2)
library(lubridate)
library(xts)
library(gridExtra)
require(dplyr)

# Read Data testset
data <- fread("data-mining-project/testset.csv")
data <- data[, .(time, lineId, pointId,  directionId, distanceFromPoint)]
data[, time := as.POSIXct(time, format = "%Y-%m-%d %H:%M:%OS")]
str(data)

# Read coordinates and actu stops 
stops_data <- fread("Downloads/all_stops.csv")
stops_data[, stop_id := as.integer(stop_id)]
stops_data <- stops_data[numero_lig == 95, ]

# Example Scenario Line
testset <- data[ lineId == 95,]
summary(testset)
dt <- testset[directionId == 4318, ]
time_to_check <-unique(dt[, time ])

# Function checking conditions
check_vehicles_conditions <-
  function(testset, time_to_check, stops) {
    # Initialize the lists
    list_speed <- list()
    
    for (j in 1:(length(time_to_check)- 1)) {
      
      # current line
      current_check <-testset[time == time_to_check[j + 1], ]  #keep attention on what we check!
      print(paste0("ALL CURRENT CHECK ", j))
      print(current_check)
      
      for (i in 1:nrow(current_check)) {
        
        print(paste0('Processing ', i , ' out of ', nrow(current_check)))

      current_data <- testset[time == time_to_check[j + 1], ][i]
      print('Checking current data: ')
      print(current_data)
      setnames(current_data, 'pointId', 'stop_id')
      
      # data past 30" seconds
      print('checking past data:')
      past_data <- testset[time == time_to_check[j],]
      print(past_data)
      setnames(past_data, 'pointId', 'stop_id')
      
      # conditions pointId
      terminus <-
        stops[stop_id == current_data[, directionId],][, succession]
      variante <-
        stops[stop_id == current_data[, directionId],][, Variante]
      check_stops_dt <- stops[Variante ==  variante,]
      
      # merging successions stop id
      successions_past_dt <-
        merge(check_stops_dt[, .(succession, stop_id)], past_data[, .(time, stop_id, distanceFromPoint)], by = 'stop_id')
      successions_current_dt <-
        merge(current_data[, .(stop_id)], check_stops_dt[, .(succession, stop_id)], by = 'stop_id')
      

      # select right succession
      successions_past_dt[, check_right_succession := succession - successions_current_dt[, succession]]
      successions_past_dt[, filter_cond := ifelse(check_right_succession < 0, 0, 1)] # keep attention here to check the cond. 
  
      # check final selection
      final_selection <- successions_past_dt[filter_cond  == 1, ]
      final_selection
      
      if (!nrow(final_selection) == 0) {
        
        for (k in 1:nrow(final_selection)) {
          
        # computation speed real time
        time_seconds <-  as.integer(current_data[ , time]) - as.integer(final_selection[, time][k]) #seconds
        distance_mtr <- current_data[,distanceFromPoint] + final_selection[, distanceFromPoint][k] #meters
        speed_real_time <- (distance_mtr/1000)/(time_seconds/3600) # km/h
        print(paste0("The speed real time is ", round(speed_real_time, digits  = 3), "km/h"))
        
        # concat results
        list_speed[[j]] <- data.table(
          time_1 = final_selection[, time],
          time_2 = current_data[, time],
          seq_stop_1 = final_selection[ , succession],
          seq_stop_2 = successions_current_dt[ , succession],
          time_seconds = time_seconds,
          distance_mtr = distance_mtr,
          speed_real_time = speed_real_time)

        }
        
      } else {
        print('not founded the right vehicle!!')}
      }
    }
    list_out <- do.call('rbind', list_speed)
    return(list_out)
    
  }

# Output results speed
results_speed <- 
  check_vehicles_conditions(testset = dt, time_to_check = time_to_check, stops = stops_data)
final_result <- results_speed[ seq_stop_2 >= seq_stop_1, ] # add cond. in the function
final_result[speed_real_time >= 100, ]



# Plot the speed distribution
ggplot(data = final_result, aes(x = ))




