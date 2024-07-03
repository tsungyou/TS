
from numpy import cos, sin
import numpy as np
import pandas as pd

class MethematicFunctions(object):
    def __init__(self):
        pass

    @staticmethod
    def trigfit(x, n, w, m, a, b, xm):
        Sc =0.0
        Ss =0.0
        Scc=0.0
        Sss=0.0
        Scs=0.0
        Sx =0.0
        Sxc=0.0
        Sxs=0.0
        for i in range(n):
            c = cos(w*i)
            s = sin(w*i)
            dx = x[i] - xm[i]
            Sc +=c
            Ss +=s
            Scc+=c*c
            Sss+=s*s
            Scs+=c*s
            Sx +=dx
            Sxc+=dx*c
            Sxs+=dx*s
        Sc /=n
        Ss /=n
        Scc/=n
        Sss/=n
        Scs/=n
        Sx /=n
        Sxc/=n
        Sxs/=n   
        if w == 0 or None:
            m = Sx
            a = 0.0
            b = 0.0
        else:
            den=(Scs-Sc*Ss)**2-(Scc-Sc*Sc)*(Sss-Ss*Ss)
            a=((Sxs-Sx*Ss)*(Scs-Sc*Ss)-(Sxc-Sx*Sc)*(Sss-Ss*Ss))/den
            b=((Sxc-Sx*Sc)*(Scs-Sc*Ss)-(Sxs-Sx*Ss)*(Scc-Sc*Sc))/den
            m=Sx-a*Sc-b*Ss
        return w, m, a ,b 

    @staticmethod
    def freq(self, x, n, w, m, a, b, xm):
        FreqTOL =0.00001
        z = [None] * n
        alpha = 0.0 # = beta for initialization
        beta = 2.0
        z[0] = x[0] - xm[0]
        count = 0
        while abs(alpha - beta) > FreqTOL:
            alpha = beta
            z[1] = x[1] = xm[1] + alpha * z[0]
            num = z[0] * z[1]
            den = z[0] * z[0]
            for i in range(2, n):
                z[i] = x[i] - xm[i] + alpha*z[i-1] - z[i-2]
                num += z[i-1]*(z[i]+z[i-2])
                den+=z[i-1]*z[i-1]
            beta = num/den
            count += 1
            if count >= 1000:
                break
        if -1 <= beta/2.0 <= 1:
            w = np.arccos(beta/2.0)
        else:
            w= 0 
        w, m, a, b = self.trigfit(x, n, w, m, a, b, xm)
        return w, m, a, b

    @staticmethod
    def fft_predict(self, adj_close):
        '''
        adj_close should be an array
        '''
        shop = adj_close
        av = 0.0

        past = 252
        pred = 100
        # past closed price
        x = [None] * past
        close = shop
        close = close[::-1]
        # average price for the past 300(past) days
        xm = [None] * past
        ym = [None] * 101
        for i in range(past):
            x[i] = close.iloc[i]
            av += x[i]
        av/=past

        for i in range(past):
            xm[i] = av
            if(i <= pred):
                ym[i] = av
        harmonics = 20
        w = 0.0
        m = 0.0
        a = 0.0
        b = 0.0
        for i in range(1, harmonics + 1):
            w, m, a, b = self.freq(x, past, w, m, a, b, xm)
            for i in range(past):
                xm[i] += m+a*cos(w*i) + b*sin(w*i)
                if(i <= pred):
                    ym[i] += m+a*cos(w*i)-b*sin(w*i)

        return ym

    @classmethod
    def fft_factorize(cls):
        pass