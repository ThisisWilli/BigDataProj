package streaming.kafka.bean;

/**
 * \* project: SparkStudy
 * \* package: streaming.kafka.bean
 * \* author: Willi Wei
 * \* date: 2019-12-23 15:11:49
 * \* description:
 * \
 */
public class Person {
    private String userId;
    private String ageRange;
    private String gender;
    private String merchantId;
    private String label;

    public Person() {
    }

    public Person(String userId, String ageRange, String gender, String merchantId, String label) {
        this.userId = userId;
        this.ageRange = ageRange;
        this.gender = gender;
        this.merchantId = merchantId;
        this.label = label;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAgeRange() {
        return ageRange;
    }

    public void setAgeRange(String ageRange) {
        this.ageRange = ageRange;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "Person{" +
                "userId='" + userId + '\'' +
                ", ageRange='" + ageRange + '\'' +
                ", gender='" + gender + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", label='" + label + '\'' +
                '}';
    }
}