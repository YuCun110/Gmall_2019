package com.caihua.bean;

import java.util.List;
import java.util.Objects;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class Teacher02 {
    private String t_id;
    private String name;
    private double salary;
    private String gender;
    private List<String> desc;

    public Teacher02() {
    }

    public Teacher02(String t_id, String name, double salary, String gender, List<String> desc) {
        this.t_id = t_id;
        this.name = name;
        this.salary = salary;
        this.gender = gender;
        this.desc = desc;
    }

    public String getT_id() {
        return t_id;
    }

    public String getName() {
        return name;
    }

    public double getSalary() {
        return salary;
    }

    public String getGender() {
        return gender;
    }

    public List<String> getDesc() {
        return desc;
    }

    public void setT_id(String t_id) {
        this.t_id = t_id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setDesc(List<String> desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "t_id='" + t_id + '\'' +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                ", gender='" + gender + '\'' +
                ", desc=" + desc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Teacher02 teacher = (Teacher02) o;
        return Double.compare(teacher.salary, salary) == 0 &&
                Objects.equals(t_id, teacher.t_id) &&
                Objects.equals(name, teacher.name) &&
                Objects.equals(gender, teacher.gender) &&
                Objects.equals(desc, teacher.desc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t_id, name, salary, gender, desc);
    }
}
