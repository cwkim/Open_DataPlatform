import math

def get_angle(_position1_lat, _position1_long, _position2_lat, _position2_long) :
    DEGREES = 180
    PI = 3.141592
    Current_LAT_Radian = _position1_lat * (PI / DEGREES)
    Current_Long_Radian = _position1_long * (PI / DEGREES)

    Destination_LAT_Radian = _position2_lat * (PI / DEGREES)
    Destination_Long_Radian = _position2_long * (PI / DEGREES)

    radianDistance = 0

    A = math.sin(Current_LAT_Radian) * math.sin(Destination_LAT_Radian)
    B = math.cos(Current_LAT_Radian) * math.cos(Destination_LAT_Radian) * math.cos(Current_Long_Radian - Destination_Long_Radian)
    C = A + B
    
    if(C > float(1)):
        C = float(1)
            
    radianDistance = math.acos(C)
    GPSAngle = 0

    try:
        GPSAngle = math.acos((math.sin(Destination_LAT_Radian)-math.sin(Current_LAT_Radian)*math.cos(radianDistance)) / 
                (math.cos(Current_LAT_Radian)*math.sin(radianDistance)))
        resultGpsAngle = 0
        if (math.sin(Destination_Long_Radian - Current_Long_Radian)<0) :
            resultGpsAngle = GPSAngle*(DEGREES/PI)
            resultGpsAngle = 360 - resultGpsAngle
        else:
            resultGpsAngle = GPSAngle * (DEGREES/PI)
    except:

        GPSAngle = 0
        resultGpsAngle = 0
        if (math.sin(Destination_Long_Radian - Current_Long_Radian)<0) :
            resultGpsAngle = GPSAngle*(DEGREES/PI)
            resultGpsAngle = 360 - resultGpsAngle
        else:
            resultGpsAngle = GPSAngle * (DEGREES/PI)
    
    return resultGpsAngle