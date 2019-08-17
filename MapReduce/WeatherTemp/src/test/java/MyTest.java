import org.apache.hadoop.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * \* Project: WeatherTemp
 * \* Package: PACKAGE_NAME
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-16 13:25:58
 * \* Description:
 * \
 */
public class MyTest {
    public static void main(String[] args) {
        String data = "1949-10-01 14:21:02,34C";
        String[] words = StringUtils.split(data.toString(), ','); // 定位制导符，切割时间和温度
        for (String w: words) {
            System.out.println(w);
        }
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        System.out.println(sdf.toString());
        try {
            Date date = sdf.parse(words[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            System.out.println(cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH));
            // 处理日期
            //System.out.println(cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH));
//            tkey.setYear(cal.get(Calendar.YEAR) - 50);
//            tkey.setMonth(cal.get(Calendar.MONTH) + 1);
//            tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
//
//            // 切割出温度
              int temp = Integer.parseInt(words[2].substring(0, words[1].lastIndexOf("c")));
              //System.out.println(temp);
//            tkey.setTemp(temp);
//            tval.set(temp);
//            context.write(tkey, tval);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}