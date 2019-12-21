// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import router from './router'
import iView from 'iview' // 引入iview依赖
import 'iview/dist/styles/iview.css' // 引入iview css样式

var axios = require('axios');
axios.defaults.baseURL = 'http://localhost:9090/api';
axios.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded';

// 将API方法绑定到全局
Vue.prototype.$axios = axios;

Vue.config.productionTip = false;

Vue.use(iView);

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  components: {App},
  template: '<App/>'
})
