import Vue from 'vue'
import Router from 'vue-router'
import BlogLogin from '@/components/manage/BlogLogin'
import BlogIndex from '@/components/home/BlogIndex'

Vue.use(Router);

export default new Router({
  mode: 'history',
  routes: [
    {
      path: '/',
      name: 'BlogLogin',
      component: BlogLogin
    },
    {
      path: '/index',
      name: 'BlogIndex',
      component: BlogIndex
    }
  ]
});
