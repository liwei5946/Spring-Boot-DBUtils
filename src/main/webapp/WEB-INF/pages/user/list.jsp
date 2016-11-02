<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<%@ include file="../../utilpage/taglibs.jsp"%>
<!DOCTYPE html>
<html>
<head>
<title>用户列表</title>
</head>
<body>
	<table id="dynamic-table">
		<thead>
			<tr>
				<th>编号</th>
				<th>名称</th>
				<th>密码</th>
				<th>操作</th>
			</tr>
		</thead>

		<tbody>
			<c:if test="${user == null}">
				<tr>
					<td colspan="4">没有数据</td>
				</tr>
			</c:if>
			<c:forEach var="user" items="${user }">
				<tr>
					<td>${user.id}</td>
					<td>${user.user_name}</td>
					<td>${user.pass_word}</td>
					<td>
						<a class="red" href="${ctx}/user/delete?id=${user.id}">Link</a>
					</td>
				</tr>
			</c:forEach>
		</tbody>
	</table>
</body>
</html>