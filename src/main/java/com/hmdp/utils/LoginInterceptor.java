package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        //1.获取session，得到session中的用户
        HttpSession session = request.getSession();
        UserDTO user = (UserDTO) session.getAttribute("user");
        //2.判断用户是否存在，不存在进行拦截，返回状态码401；存在则存入threadLocal中，进行放行
        if (user == null) {
            response.setStatus(401);
            return false;
        }
        UserHolder.saveUser(user);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
