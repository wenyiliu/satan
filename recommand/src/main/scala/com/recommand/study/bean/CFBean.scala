package com.recommand.study.bean

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
// 用户收听音乐详情 bean
case class UserListenDetailBean(userId: String, musicId: String, remaintime: String, durationHour: String)

// 用户对某个音乐的喜爱程度
case class UserMusicFavoriteRate(userId: String, musicId: String, favoriteRate: Double)

case class ALSParam(maxIter: Int, regParam: Double, rank: Int, uid: String, iid: String, rating: String)

