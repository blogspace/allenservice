package demo;

import java.util.Arrays;

public class FlinkDemo {
    public static void main(String[] args) {
        int[] demo = {2, 1, 4, 5, 3, 0};
        quick(demo);
        System.out.println(Arrays.toString(demo));
    }


    public static void quick(int[] nums) {
        int mid = nums[0];
        int tmp = 0;
        int big = 0;
        int small = 0;
        int i = 0;
        int j = nums.length - 1;
        while (i < j) {
            if (nums[i] > mid) {
                i++;

            }else if(nums[j] < mid){
                j--;
            }
            int swap=nums[i];
            nums[i]=nums[j];
            nums[j]=swap;


        }


    }

    public static void removeDuplicates(int[] nums) {
        int small = 0;
        int big = 0;
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < nums.length - 1; j++) {
                if (nums[j + 1] < nums[j]) {
                    big = nums[j];
                    small = nums[j + 1];
                    nums[j + 1] = big;
                    nums[j] = small;

                }

            }
        }
    }

    public static void BubbleSort(int[] arr) {
        int temp;//定义一个临时变量
        for (int i = 0; i < arr.length - 1; i++) {//冒泡趟数
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (arr[j + 1] < arr[j]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                    System.out.println(i + " " + j);
                }
            }
        }
    }

}
