<!-- 这是登录页 -->
<template>
  <div>
    <header></header>
    <div>这是登录页</div>
    <template>
      <div>
        <Form ref="user" :model="user" :rules="ruleInline" inline>
          <div>
            <FormItem prop="username">
              <Input type="text" v-model="user.username" placeholder="请输入用户名">
                <Icon type="ios-person-outline" slot="prepend"></Icon>
              </Input>
            </FormItem>
          </div>
          <div>
            <FormItem prop="password">
              <Input type="password" v-model="user.password" placeholder="请输入密码">
                <Icon type="ios-lock-outline" slot="prepend"></Icon>
              </Input>
            </FormItem>
          </div>
          <div>
            <FormItem>
              <Button type="primary" @click="login()">登录</Button>
            </FormItem>
          </div>
        </Form>
      </div>
    </template>
    <footer></footer>
  </div>
</template>

<script>
  import Header from '../common/BlogFooter'
  import Footer from '../common/BlogHeader'
  var qs = require('qs');

  export default {
    components: {
      Header,
      Footer
    },
    data() {
      return {
        user: {username: '', password: ''},
        ruleInline: {
          username: [
            {required: true, message: '用户名不能为空', trigger: 'blur'}],
          password: [
            {required: true, message: '密码不能为空', trigger: 'blur'},
            {type: 'string', min: 6, message: '密码长度不能小于6', trigger: 'blur'}
          ]
        },
      }
    },
    methods: {
      login() {
        this.$axios.post('/login',qs.stringify(this.user) ,{headers: {'Content-Type': 'application/x-www-form-urlencoded'}})
          .then(response => {
            if (response.data === 'success') {
              this.$router.replace({path: '/index'});
            } else {
              alert("输入错误");
             // console.log("alert:"+alert(response.data))
            }
          }).catch(error => {
          console.log("error" + error);
        });
      }

    }
  }
</script>

<style lang="">

</style>
