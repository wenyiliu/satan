package com.satan.hadoop;

import com.satan.hadoop.annotion.Value;
import com.satan.hadoop.utils.ValueUtils;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class Test {

    @Value("fs.defaultFS")
    private String name;

    public int getAa() {
        return aa;
    }

    public void setAa(int aa) {
        this.aa = aa;
    }

    @Value("aa")
    private int aa;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void main(String[] args) throws Exception {
        Test test = new Test();
        ValueUtils.getValue(test);
        System.out.println(test.getName());
        System.out.println(test.getAa());
    }
}
