package com.satan.hive.customFunctation.udf;

import java.io.Serializable;
import java.util.List;

/**
 * @author liuwenyi
 * @date 2020/12/23
 */
public class Test {
    /**
     * moduleType : 2
     * indexList : [{"name":"并发症情况","unit":"例","data":[{"name":"切口感染","value":"10","total":"100"}]}]
     */

    private int moduleType;
    private List<IndexListBean> indexList;

    public int getModuleType() {
        return moduleType;
    }

    public void setModuleType(int moduleType) {
        this.moduleType = moduleType;
    }

    public List<IndexListBean> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<IndexListBean> indexList) {
        this.indexList = indexList;
    }

    public static class IndexListBean implements Serializable {
        /**
         * name : 并发症情况
         * unit : 例
         * data : [{"name":"切口感染","value":"10","total":"100"}]
         */

        private String name;
        private String unit;
        private List<DataBean> data;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }

        public List<DataBean> getData() {
            return data;
        }

        public void setData(List<DataBean> data) {
            this.data = data;
        }

        public static class DataBean implements Serializable {
            /**
             * name : 切口感染
             * value : 10
             * total : 100
             */

            private String name;
            private String value;
            private String total;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getValue() {
                return value;
            }

            public void setValue(String value) {
                this.value = value;
            }

            public String getTotal() {
                return total;
            }

            public void setTotal(String total) {
                this.total = total;
            }
        }
    }
}
