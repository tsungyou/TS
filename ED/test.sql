     "WITH Returns AS ( " + \
     
            "    SELECT " + \
            "        DA, " + \
            "        CODE, " + \
            "        ((HI+LO)/2 / LAG((HI+LO)/2, 1) OVER (PARTITION BY CODE ORDER BY DA)) - 1 AS Return " + \
            "    FROM " + \
            "        PRICE " + \
            "    WHERE " + \
            "        CODE IN ('TWSE Index', 'TWOTCI Index') " + \
            ") " + \
            "SELECT " + \
            "    P1.DA, " + \
            "    (SELECT CL FROM PRICE WHERE CODE = 'TWSE Index' AND DA > P1.DA ORDER BY DA ASC LIMIT 1) / P1.CL - 1 AS TWSE_BETA, " + \
            "    (SELECT CL FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA > P1.DA ORDER BY DA ASC LIMIT 1) / " + \
            "    (SELECT CL FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA = P1.DA ORDER BY DA ASC LIMIT 1) - 1 AS TWOTCI_BETA, " + \
            "    (SELECT STDDEV(Return)*100 FROM Returns WHERE CODE = 'TWSE Index' AND DA BETWEEN P1.DA - INTERVAL '5 days' AND P1.DA) AS TWSE_RETURN_STDDEV, " + \
            "    (SELECT STDDEV(Return)*100 FROM Returns WHERE CODE = 'TWOTCI Index' AND DA BETWEEN P1.DA - INTERVAL '5 days' AND P1.DA) AS TWOTCI_RETURN_STDDEV, " + \
            "    (SELECT AVG(CL) FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA IN (SELECT DA FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA <= P1.DA ORDER BY DA DESC LIMIT 20) ),"+\
            "    (SELECT AVG(CL) FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA IN (SELECT DA FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA <= P1.DA ORDER BY DA DESC LIMIT 60) ),"+\
            "    (SELECT CL FROM PRICE WHERE CODE = 'TWOTCI Index' AND DA = P1.DA), "+ \
                "    (SELECT AVG(CL) FROM PRICE WHERE CODE = 'TWSE Index' AND DA IN (SELECT DA FROM PRICE WHERE CODE = 'TWSE Index' AND DA <= P1.DA ORDER BY DA DESC LIMIT 20) )," + \
                "    (SELECT AVG(CL) FROM PRICE WHERE CODE = 'TWSE Index' AND DA IN (SELECT DA FROM PRICE WHERE CODE = 'TWSE Index' AND DA <= P1.DA ORDER BY DA DESC LIMIT 60) )," + \
                "    cl " + \
                "FROM " + \
            "    PRICE AS P1 " + \
            "WHERE " + \
            "    P1.CODE = 'TWSE Index' " + \
            "    AND P1.DA >= %s " + \
            "ORDER BY " + \
            "    P1.DA ASC"


  if std_otc <= 3 and cl_otc > sma20_otc:
                fund3_timing_stock_beta = fund1_timing_stock_beta
                real_exposure.append(1)
                print(str(da)+' '+str(std_otc) + " 1 " + str(std_otc-pre_std_otc))
                flag = "波動牛📈"
                flag_singal = 1;
            elif std_otc <= 1 or std_twii >= 3.5:
                fund3_timing_stock_beta = fund1_timing_stock_beta
                real_exposure.append(1)
                print(str(da)+' '+str(std_otc) + " 1 " + str(std_otc-pre_std_otc))
                flag = "穩定牛📈"
                flag_singal = 1;
            elif (std_otc-pre_std_otc) > 0.5 and cl_otc > sma60_otc and cl_otc < sma20_otc:
                fund3_timing_stock_beta = fund1_timing_stock_beta
                real_exposure.append(1)
                print(str(da)+' '+str(std_otc) + " 1 " + str(std_otc-pre_std_otc))
                flag = "抄底牛📈"
                flag_singal = 1;
            else:
                fund3_timing_stock_beta = 0
                real_exposure.append(0)
                print(str(da)+' '+str(std_otc) + " 0 "+ str(std_otc-pre_std_otc))
                flag = "害怕熊📉"
                flag_singal = 0;