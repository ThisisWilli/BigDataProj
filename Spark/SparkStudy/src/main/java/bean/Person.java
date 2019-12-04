package bean;

/**
 * \* project: SparkStudy
 * \* package: bean
 * \* author: Willi Wei
 * \* date: 2019-12-04 11:23:16
 * \* description:
 * \
 */
public class Person {
    private String name;
    private String sex;
    private String age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                '}';
    }
}