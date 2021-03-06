
import java.io.Serializable;
import java.util.Date;

public class Recruit implements Serializable {
    private Long ID;
    private String companyName;
    private String jobName;
    private String jobLabel;
    private Integer useFlag;
    private Date publishDate;
    private String positionName;

    public Recruit(Long ID, String companyName, String jobName, String jobLabel, Integer useFlag, Date publishDate, String positionName, String JOB_SALARY, String maxSalary, String minSalary) {
        this.ID = ID;
        this.companyName = companyName;
        this.jobName = jobName;
        this.jobLabel = jobLabel;
        this.useFlag = useFlag;
        this.publishDate = publishDate;
        this.positionName = positionName;
        this.JOB_SALARY = JOB_SALARY;
        this.maxSalary = maxSalary;
        this.minSalary = minSalary;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public Recruit(String companyName, String positionName, String JOB_SALARY) {
        this.companyName = companyName;
        this.positionName = positionName;
        this.JOB_SALARY = JOB_SALARY;
    }

    public Recruit(String companyName, String positionName) {
        this.companyName = companyName;
        this.positionName = positionName;
    }

    public Recruit(String companyName) {
        this.companyName = companyName;
    }


    public Long getID() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobLabel() {
        return jobLabel;
    }

    public void setJobLabel(String jobLabel) {
        this.jobLabel = jobLabel;
    }

    public Integer getUseFlag() {
        return useFlag;
    }

    public void setUseFlag(Integer useFlag) {
        this.useFlag = useFlag;
    }

    public Date getPublishDate() {
        return publishDate;
    }

    public void setPublishDate(Date publishDate) {
        this.publishDate = publishDate;
    }

    public String getJOB_SALARY() {
        return JOB_SALARY;
    }

    public void setJOB_SALARY(String JOB_SALARY) {
        this.JOB_SALARY = JOB_SALARY;
    }

    public String getMaxSalary() {
        return maxSalary;
    }

    public void setMaxSalary(String maxSalary) {
        this.maxSalary = maxSalary;
    }

    public String getMinSalary() {
        return minSalary;
    }

    public void setMinSalary(String minSalary) {
        this.minSalary = minSalary;
    }

    private String JOB_SALARY;
    private String maxSalary;
    private String minSalary;

    @Override
    public String toString() {
        return "Recruit{" +
                "ID=" + ID +
                ", companyName='" + companyName + '\'' +
                ", jobName='" + jobName + '\'' +
                ", jobLabel='" + jobLabel + '\'' +
                ", useFlag=" + useFlag +
                ", publishDate=" + publishDate +
                ", JOB_SALARY='" + JOB_SALARY + '\'' +
                ", maxSalary='" + maxSalary + '\'' +
                ", minSalary='" + minSalary + '\'' +
                '}';
    }
}
