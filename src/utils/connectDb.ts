import mongoose from "mongoose";

export const connectDb = async (dbUrl: string) => {
  try {
    await mongoose.connect(dbUrl);
    console.log("[::] ► Database connected ♪.♪..♫...♫.♪.♪..♫...♫");
  } catch (error) {
    console.log("[::] ► Database not connected !!!");
  }
};
